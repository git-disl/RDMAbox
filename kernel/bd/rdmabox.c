/*
 * rdmabox, remote memory paging over RDMA
 *
 * Copyright (c) 2013 Mellanox Technologies��. All rights reserved.
 *
 * This software is available to you under a choice of one of two licenses.
 * You may choose to be licensed under the terms of the GNU General Public
 * License (GPL) Version 2, available from the file COPYING in the main
 * directory of this source tree, or the Mellanox Technologies�� BSD license
 * below:
 *
 *      - Redistribution and use in source and binary forms, with or without
 *        modification, are permitted provided that the following conditions
 *        are met:
 *
 *      - Redistributions of source code must retain the above copyright
 *        notice, this list of conditions and the following disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the followingu
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 *      - Neither the name of the Mellanox Technologies�� nor the names of its
 *        contributors may be used to endorse or promote products derived from
 *        this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "debug.h"

#include <linux/time.h>

#include "rpg_drv.h"
#include "rdmabox.h"
//#include "diskbox.h"
//#include "zrambox.h"
#include "radixtree.h"
#include "rpg_mempool.h"
#include "alf_queue.h"

//struct sysinfo infomem;

#define DEBUG

int NUM_CB;	// num of server/cb
int RDMABOX_indexes; /* num of devices created*/
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
extern struct bio_set *io_bio_set;
#endif

#ifdef FLOW_CONTROL
atomic_t num_outstanding;
#define MAX_NUM_OUTSTANDING 472
#endif
// wr rd batch 512k
// 576 : 20312
// 512 : 27343 , 19031
// 480 : 32646, 20064, 21656, 20851, 28554
// 472 :
// 464 : 23188, 25212
// 448 : 23056

// SGE
// 576 : 24364
// 512 : 20725
// 480 : 21624
//472 : 28457
//464 : 22912
//448 : 21409

int req_map_sg_each(struct request *req, struct rdma_ctx *tmp, struct scatterlist *sglist)
{
    int nents;
    nents = blk_rq_map_sg(req->q, req, sglist);
    //printk("rdmabox[%s] : mapped sg nents : %d\n",__func__, nents);
    if (nents <= 0) {
	printk("rdmabox[%s] : mapped sg nents : %d\n",__func__, nents);
	return -EINVAL;
    }
    tmp->data_tbl.nents = tmp->data_tbl.nents + nents;

    return 0;
}

int req_map_sg(struct request *req, struct rdma_ctx *tmp)
{
    tmp->data_tbl.nents = blk_rq_map_sg(req->q, req, tmp->data_tbl.sgl);
    //printk("rdmabox[%s] : mapped sg nents : %d\n",__func__, tmp->data_tbl.nents);
    if (tmp->data_tbl.nents <= 0) {
	printk("rdmabox[%s] : mapped sg nents : %d\n",__func__, tmp->data_tbl.nents);
	return -EINVAL;
    }

    return 0;
}

int isRDMAon(struct RDMABOX_session *RDMABOX_sess)
{
    if(atomic_read(&RDMABOX_sess->rdma_on) == DEV_RDMA_ON)
	return 1;
    else
	return 0;
}

void RDMABOX_bitmap_set(int *bitmap, int i)
{
    bitmap[i >> BITMAP_SHIFT] |= 1 << (i & BITMAP_MASK);
}

void RDMABOX_bitmap_group_set(int *bitmap, unsigned long offset, unsigned long len)
{
    int start_page = (int)(offset/RDMABOX_PAGE_SIZE);
    int len_page = (int)(len/RDMABOX_PAGE_SIZE);
    int i;
    for (i=0; i<len_page; i++){
	RDMABOX_bitmap_set(bitmap, start_page + i);
    }
}

void RDMABOX_bitmap_group_clear(int *bitmap, unsigned long offset, unsigned long len)
{
    int start_page = (int)(offset/RDMABOX_PAGE_SIZE);
    int len_page = (int)(len/RDMABOX_PAGE_SIZE);
    int i;
    for (i=0; i<len_page; i++){
	RDMABOX_bitmap_clear(bitmap, start_page + i);
    }
}

bool RDMABOX_bitmap_test(int *bitmap, int i)
{
    if ((bitmap[i >> BITMAP_SHIFT] & (1 << (i & BITMAP_MASK))) != 0){
	return true;
    }else{
	return false;
    }
}

void RDMABOX_bitmap_clear(int *bitmap, int i)
{
    bitmap[i >> BITMAP_SHIFT] &= ~(1 << (i & BITMAP_MASK));
}

void RDMABOX_bitmap_init(int *bitmap)
{
    memset(bitmap, 0x00, ONE_GB/(4096*8));
}

int get_both(struct RDMABOX_session *RDMABOX_sess, void **ctx_element, void **mr_element, int cbhint, int cpuhint, int nctx, int nmr, int isWrite)
{
    int startcpu = cpuhint;
    struct rdma_ctx *ctx;
    struct rdma_mr *mr;
    cbhint = cbhint % NUM_POOL;
    if(cbhint != 0)
	panic("modulo is not 0");

retry_both:
    if(isWrite){
	get_freepool_both(RDMABOX_sess->cb_list[cbhint]->rdma_write_freepool_list[cpuhint],ctx_element,mr_element,&nctx,&nmr);
    }else{
	get_freepool_both(RDMABOX_sess->cb_list[cbhint]->rdma_read_freepool_list[cpuhint],ctx_element,mr_element,&nctx,&nmr);
    }
    //printk("rdmabox[%s]: nctx:%d nmr:%d \n",__func__,nctx,nmr);
    if(nctx == 0 && nmr == 0){
	ctx = (struct rdma_ctx *)*ctx_element;
	mr = (struct rdma_mr *)*mr_element;
	ctx->cpuhint = cpuhint;
	ctx->cbhint = cbhint;
	mr->cpuhint = cpuhint;
	mr->cbhint = cbhint;
    }else if(nctx != 0 && nmr == 0){
	mr = (struct rdma_mr *)*mr_element;
	mr->cpuhint = cpuhint;
	mr->cbhint = cbhint;
	cpuhint++;
	if(cpuhint == submit_queues)
	    cpuhint = 0;
	if(cpuhint == startcpu){
	    cbhint++;
	    //if(cbhint == RDMABOX_sess->cb_num)
	    if(cbhint == NUM_POOL)
		cbhint=0;
	}
retry_ctx:
	if(isWrite)
	    get_freepool_single(RDMABOX_sess->cb_list[cbhint]->rdma_write_freepool_list[cpuhint],ctx_element,NULL,&nctx,&nmr);
	else
	    get_freepool_single(RDMABOX_sess->cb_list[cbhint]->rdma_read_freepool_list[cpuhint],ctx_element,NULL,&nctx,&nmr);
	if(nctx != 0){
	    cpuhint++;
	    if(cpuhint == submit_queues)
		cpuhint = 0;
	    if(cpuhint == startcpu){
		cbhint++;
		//if(cbhint == RDMABOX_sess->cb_num)
		if(cbhint == NUM_POOL)
		    cbhint=0;
	    }
	    goto retry_ctx;
	}
	ctx = (struct rdma_ctx *)*ctx_element;
	ctx->cpuhint = cpuhint;
	ctx->cbhint = cbhint;
    }else if(nctx == 0 && nmr != 0){
	ctx = (struct rdma_ctx *)*ctx_element;
	ctx->cpuhint = cpuhint;
	ctx->cbhint = cbhint;
	cpuhint++;
	if(cpuhint == submit_queues)
	    cpuhint = 0;
	if(cpuhint == startcpu){
	    cbhint++;
	    //if(cbhint == RDMABOX_sess->cb_num)
	    if(cbhint == NUM_POOL)
		cbhint=0;
	}
retry_mr:
	if(isWrite)
	    get_freepool_single(RDMABOX_sess->cb_list[cbhint]->rdma_write_freepool_list[cpuhint],NULL,mr_element,&nctx,&nmr);
	else
	    get_freepool_single(RDMABOX_sess->cb_list[cbhint]->rdma_read_freepool_list[cpuhint],NULL,mr_element,&nctx,&nmr);
	if(nmr != 0){
	    //printk("rdmabox[%s]: try to get ctx[%d] \n",__func__,cpuhint);
#if defined(SUPPORT_HYBRID_WRITE) || defined(SUPPORT_HYBRID_READ)
	    return 1;
#else
	    cpuhint++;
	    if(cpuhint == submit_queues)
		cpuhint = 0;
	    if(cpuhint == startcpu){
		cbhint++;
		//if(cbhint == RDMABOX_sess->cb_num)
		if(cbhint == NUM_POOL)
		    cbhint=0;
	    }
	    goto retry_mr;
#endif
	}
	mr = (struct rdma_mr *)*mr_element;
	mr->cpuhint = cpuhint;
	mr->cbhint = cbhint;
    }else{
	cpuhint++;
	if(cpuhint == submit_queues)
	    cpuhint = 0;
	if(cpuhint == startcpu){
	    cbhint++;
	    //if(cbhint == RDMABOX_sess->cb_num)
	    if(cbhint == NUM_POOL)
		cbhint=0;
	}
	goto retry_both;
    }

    return 0;
}

int get_single(struct RDMABOX_session *RDMABOX_sess , void **ctx_element, void **mr_element, int cbhint, int cpuhint, int nctx, int nmr, int isWrite)
{
    int startcpu = cpuhint;
    struct rdma_ctx *ctx;
    struct rdma_mr *mr;
    int i;
    cbhint = cbhint % NUM_POOL;
    if(cbhint != 0)
	panic("modulo is not 0");

    if(ctx_element != NULL){
retry_ctx:
	if(isWrite){
	    get_freepool_single(RDMABOX_sess->cb_list[cbhint]->rdma_write_freepool_list[cpuhint],ctx_element,NULL,&nctx,&nmr);
	}else{
	    get_freepool_single(RDMABOX_sess->cb_list[cbhint]->rdma_read_freepool_list[cpuhint],ctx_element,NULL,&nctx,&nmr);
	}
	//printk("rdmabox[%s]: nctx:%d \n",__func__,nctx);
	if(nctx != 0){
	    //printk("rdmabox[%s]: try to get ctx[%d] \n",__func__,cpuhint);
	    cpuhint++;
	    if(cpuhint == submit_queues)
		cpuhint = 0;
	    if(cpuhint == startcpu){
		cbhint++;
		//if(cbhint == RDMABOX_sess->cb_num)
		if(cbhint == NUM_POOL)
		    cbhint=0;
	    }
	    goto retry_ctx;
	}
	ctx = (struct rdma_ctx *)*ctx_element;
	ctx->cpuhint = cpuhint;
	ctx->cbhint = cbhint;
    }

    if(mr_element != NULL){
retry_mr:
	if(isWrite){
	    get_freepool_single(RDMABOX_sess->cb_list[cbhint]->rdma_write_freepool_list[cpuhint],NULL,mr_element,&nctx,&nmr);
	}else{
	    get_freepool_single(RDMABOX_sess->cb_list[cbhint]->rdma_read_freepool_list[cpuhint],NULL,mr_element,&nctx,&nmr);
	}
	if(nmr != 0){
	    //printk("rdmabox[%s]: try to get mr[%d] \n",__func__,cpuhint);
#if defined(SUPPORT_HYBRID_WRITE) || defined(SUPPORT_HYBRID_READ)
	    return 1;
#else
	    cpuhint++;
	    if(cpuhint == submit_queues)
		cpuhint = 0;
	    if(cpuhint == startcpu){
		cbhint++;
		//if(cbhint == RDMABOX_sess->cb_num)
		if(cbhint == NUM_POOL)
		    cbhint=0;
	    }
	    goto retry_mr;
#endif
	}
	mr = (struct rdma_mr *)*mr_element;
	mr->cpuhint = cpuhint;
	mr->cbhint = cbhint;
    }

    return 0;
}

void put_freepool(rdma_freepool_t *pool, void *ctx_element, void *mr_element)
{
    unsigned long flags;

    spin_lock_irqsave(&pool->rdma_freepool_lock, flags);
    if(ctx_element != NULL){
	//BUG_ON(pool->curr_nr_ctx >= pool->cap_nr_ctx);
	pool->ctx_elements[pool->curr_nr_ctx++] = ctx_element;
    }
    if(mr_element != NULL){
	//BUG_ON(pool->curr_nr_mr >= pool->cap_nr_mr);
	pool->mr_elements[pool->curr_nr_mr++] = mr_element;
    }
    spin_unlock_irqrestore(&pool->rdma_freepool_lock, flags);
}

void get_freepool_single(rdma_freepool_t *pool, void **ctx_element, void **mr_element, int *nctx, int *nmr)
{
    unsigned long flags;

    if(*nctx != 0){
	if (pool->curr_nr_ctx == 0){
	    *ctx_element = NULL;
	    return;
	}
	spin_lock_irqsave(&pool->rdma_freepool_lock, flags);
	if (pool->curr_nr_ctx > 0) {
	    //BUG_ON(pool->curr_nr_ctx <= 0);
	    *ctx_element = pool->ctx_elements[--pool->curr_nr_ctx];
	    *nctx = *nctx - 1;
	}else{
	    *ctx_element = NULL;
	}
	spin_unlock_irqrestore(&pool->rdma_freepool_lock, flags);
    }
    if(*nmr != 0){
	if (pool->curr_nr_mr == 0){
	    *mr_element = NULL;
	    return;
	}
	spin_lock_irqsave(&pool->rdma_freepool_lock, flags);
	if (pool->curr_nr_mr > 0) {
	    //BUG_ON(pool->curr_nr_mr <= 0);
	    *mr_element = pool->mr_elements[--pool->curr_nr_mr];
	    *nmr = *nmr - 1;
	}else{
	    *mr_element = NULL;
	}
	spin_unlock_irqrestore(&pool->rdma_freepool_lock, flags);
    }

    return;
}

void get_freepool_both(rdma_freepool_t *pool, void **ctx_element, void **mr_element, int *nctx, int *nmr)
{
    unsigned long flags;
    /*
       int nogo=0;

       if (pool->curr_nr_ctx == 0){
     *ctx_element = NULL;
     nogo++;
     }
     if (pool->curr_nr_mr == 0){
     *mr_element = NULL;
     nogo++;
     }
     if(nogo == 2)
     return;
     */
    spin_lock_irqsave(&pool->rdma_freepool_lock, flags);
    if (pool->curr_nr_ctx > 0) {
	//BUG_ON(pool->curr_nr_ctx <= 0);
	*ctx_element = pool->ctx_elements[--pool->curr_nr_ctx];
	*nctx = *nctx - 1;
    }else{
	*ctx_element = NULL;
    }
    if (pool->curr_nr_mr > 0) {
	//BUG_ON(pool->curr_nr_mr <= 0);
	*mr_element = pool->mr_elements[--pool->curr_nr_mr];
	*nmr = *nmr - 1;
    }else{
	*mr_element = NULL;
    }
    spin_unlock_irqrestore(&pool->rdma_freepool_lock, flags);

    return;
}

int rdma_freepool_create(struct RDMABOX_session *RDMABOX_session, struct rdma_freepool_s **cpupool, unsigned long cap_nr_ctx, unsigned long cap_nr_mr, int buf_size)
{
    int i;
    struct rdma_ctx *ctx;	
    struct rdma_mr *mr;	
    struct rdma_freepool_s *pool;

    //printk("mempool[%s]: start mempool create cap_nr=%d sizeof header: %d \n",__func__,cap_nr, sizeof(struct chunk_header));
    //printk("mempool[%s]: start mempool create cap_nr=%d \n",__func__,cap_nr);

    pool = (struct rdma_freepool_s *)kmalloc(sizeof(rdma_freepool_t), GFP_KERNEL | __GFP_ZERO);
    if (!pool){
	printk("mempool[%s]: Fail to create pool  \n",__func__);
	return 1;
    }
    *cpupool = pool;
    spin_lock_init(&pool->rdma_freepool_lock);
    pool->init_done = 0;

    pool->ctx_elements = kzalloc(cap_nr_ctx * sizeof(void *),GFP_KERNEL);
    if (!pool->ctx_elements) {
	printk("mempool[%s]: Fail to create pool elements  \n",__func__);
	kfree(pool);
	return 1;
    }
    pool->cap_nr_ctx = cap_nr_ctx;
    pool->curr_nr_ctx = 0;

    while (pool->curr_nr_ctx < pool->cap_nr_ctx) {
	ctx = (struct rdma_ctx *)kzalloc(sizeof(struct rdma_ctx), GFP_KERNEL);
	atomic_set(&ctx->in_flight, CTX_IDLE);
	ctx->chunk_index = -1;
	ctx->reqlist[0] = NULL;
	ctx->numreq = 0;
	ctx->sending_item = NULL;
	ctx->cbhint = -1;
	ctx->cpuhint = -1;
	//ctx->msg = NULL;

	put_freepool(pool,ctx,NULL);
    }//while

    pool->mr_elements = kzalloc(cap_nr_mr * sizeof(void *),GFP_KERNEL);
    if (!pool->mr_elements) {
	printk("mempool[%s]: Fail to create pool elements  \n",__func__);
	kfree(pool);
	return 1;
    }
    pool->cap_nr_mr = cap_nr_mr;
    pool->curr_nr_mr = 0;

    while (pool->curr_nr_mr < pool->cap_nr_mr) {
	mr = (struct rdma_mr *)kzalloc(sizeof(struct rdma_mr), GFP_KERNEL);
	mr->rdma_buf = kzalloc(buf_size, GFP_KERNEL);
	if (!mr->rdma_buf) {
	    printk("rdmabox[%s]: rdma_buf malloc failed\n",__func__);
	    kfree(mr->rdma_buf);
	    return 1;
	}
	mr->cbhint = -1;
	mr->cpuhint = -1;
	put_freepool(pool,NULL,mr);
    }//while

    return 0;	
}
void rdma_freepool_post_init(struct RDMABOX_session *RDMABOX_session, rdma_freepool_t *pool, int buf_size, int isWrite)
{
    int i,j;
    struct rdma_ctx *ctx;
    struct rdma_mr *mr;

    for(i=0; i<pool->cap_nr_ctx; i++){

	ctx = pool->ctx_elements[i];
	if(isWrite){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx->rdma_sq_wr.wr.opcode = IB_WR_RDMA_WRITE;
	    ctx->rdma_sq_wr.wr.send_flags = IB_SEND_SIGNALED;
	    ctx->rdma_sq_wr.wr.sg_list = &ctx->rdma_sgl_wr[0];
	    ctx->rdma_sq_wr.wr.num_sge = 1;
	    ctx->rdma_sq_wr.wr.wr_id = uint64_from_ptr(ctx);
	    ctx->rdma_sq_wr.wr.next = NULL;
#else
	    ctx->rdma_sq_wr.opcode = IB_WR_RDMA_WRITE;
	    ctx->rdma_sq_wr.send_flags = IB_SEND_SIGNALED;
	    ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl_wr[0];
	    ctx->rdma_sq_wr.num_sge = 1;
	    ctx->rdma_sq_wr.wr_id = uint64_from_ptr(ctx);
	    ctx->rdma_sq_wr.next = NULL;
#endif
	}else{//read
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx->rdma_sq_wr.wr.opcode = IB_WR_RDMA_READ;
	    ctx->rdma_sq_wr.wr.send_flags = IB_SEND_SIGNALED;
	    ctx->rdma_sq_wr.wr.sg_list = &ctx->rdma_sgl_wr[0];
	    ctx->rdma_sq_wr.wr.sg_list->length = RDMABOX_PAGE_SIZE;
	    ctx->rdma_sq_wr.wr.num_sge = 1;
	    ctx->rdma_sq_wr.wr.wr_id = uint64_from_ptr(ctx);
#else
	    ctx->rdma_sq_wr.opcode = IB_WR_RDMA_READ;
	    ctx->rdma_sq_wr.send_flags = IB_SEND_SIGNALED;
	    ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl_wr[0];
	    ctx->rdma_sq_wr.sg_list->length = RDMABOX_PAGE_SIZE;
	    ctx->rdma_sq_wr.num_sge = 1;
	    ctx->rdma_sq_wr.wr_id = uint64_from_ptr(ctx);
#endif
	}//Read
    }//for ctx

    for(i=0; i<pool->cap_nr_mr; i++){

	mr = pool->mr_elements[i];
/*
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 11, 0)
	if(isWrite){
	    mr->rdma_dma_addr = dma_map_single(&RDMABOX_session->pd->device->dev,
		    mr->rdma_buf, buf_size,
		    DMA_TO_DEVICE);
	}else{
	    mr->rdma_dma_addr = dma_map_single(&RDMABOX_session->pd->device->dev,
		    mr->rdma_buf, buf_size,
		    DMA_FROM_DEVICE);
	}
#else
	*/
	if(isWrite){
	    mr->rdma_dma_addr = dma_map_single(RDMABOX_session->pd->device->dma_device, 
		    mr->rdma_buf, buf_size, 
		    DMA_TO_DEVICE);
	}else{
	    mr->rdma_dma_addr = dma_map_single(RDMABOX_session->pd->device->dma_device, 
		    mr->rdma_buf, buf_size, 
		    DMA_FROM_DEVICE);
	}
//#endif
	pci_unmap_addr_set(mr, rdma_mapping, mr->rdma_dma_addr);	
    }//for mr

    pool->init_done = 1;   
}
static int post_send(struct kernel_cb *cb)
{
    int ret = 0;
    struct ib_send_wr * bad_wr;

    ret = ib_post_send(cb->qp[0], &cb->sq_wr, &bad_wr);
    if (ret) {
	printk("rdmabox[%s]: post_send error %d\n",__func__, ret);

	return ret;
    }

    return 0;	
}

static void RDMABOX_migration_chunk_init(struct kernel_cb *cb)
{
    int i;
    struct RDMABOX_session *RDMABOX_session = cb->RDMABOX_sess;

    int replica_index = cb->recv_buf.replica_index;
    cb->remote_chunk.client_swapspace_index = cb->recv_buf.size_gb; // client chunk index
    cb->remote_chunk.dest_cb_index = cb->recv_buf.cb_index;
    cb->remote_chunk.dest_chunk_index = cb->recv_buf.chunk_index;

    for (i=0; i<MAX_MR_SIZE_GB; i++){
	if (cb->recv_buf.rkey[i]){
	    cb->remote_chunk.migration_chunk_map[i] = 'm'; // need to migrate
	    cb->remote_chunk.chunk_list[i]->remote_rkey = ntohl(cb->recv_buf.rkey[i]);
	    cb->remote_chunk.chunk_list[i]->remote_addr = ntohll(cb->recv_buf.buf[i]);
	    cb->remote_chunk.chunk_list[i]->bitmap_g = (int *)kzalloc(sizeof(int) * BITMAP_INT_SIZE, GFP_KERNEL);
	    cb->remote_chunk.chunk_list[i]->replica_index = replica_index;
	    RDMABOX_bitmap_init(cb->remote_chunk.chunk_list[i]->bitmap_g);
	    //RDMABOX_session->free_chunk_index -= 1;
	    cb->remote_chunk.chunk_map[i] = cb->remote_chunk.client_swapspace_index;

	    cb->remote_chunk.chunk_size_g += 1;
	    atomic_set(cb->remote_chunk.remote_mapped + i, CHUNK_MAPPED);
	    cb->remote_chunk.c_state = C_MIGRATION_READY;

	    // we don't update below mr info yet. until migration is done
	    //atomic_set(RDMABOX_session->cb_index_map[0] + (select_chunk), cb->cb_index);
	    //RDMABOX_session->mapping_swapspace_to_chunk[0][select_chunk] = i;
	    //cb->remote_chunk.c_state = C_READY;

	    printk("rdmabox[%s]: Received MIGRATION dest info. chunk[%d] on server[%d] is new destination. check:%d \n", __func__,i,cb->cb_index,cb->recv_buf.size_gb);
	    printk("rdmabox[%s]: rkey:0x%x addr:0x%x \n",__func__,cb->remote_chunk.chunk_list[i]->remote_rkey,cb->remote_chunk.chunk_list[i]->remote_addr);
	}else{
	    cb->remote_chunk.migration_chunk_map[i] = 0;
	}
    }
}

void RDMABOX_single_chunk_init(struct kernel_cb *cb)
{
    int i = 0;
    struct remote_chunk_g *chunk;
    int select_chunk = cb->recv_buf.size_gb; //returning client_chunk_index
    //int replica_index = cb->recv_buf.cb_index; //returning replica_index
    int replica_index = cb->recv_buf.replica_index; //returning replica_index
    struct RDMABOX_session *RDMABOX_session = cb->RDMABOX_sess;

    for (i = 0; i < MAX_MR_SIZE_GB; i++) {
	if (cb->recv_buf.rkey[i]) { //from server, this chunk is allocated and given to you
	    cb->remote_chunk.chunk_list[i]->remote_rkey = ntohl(cb->recv_buf.rkey[i]);
	    cb->remote_chunk.chunk_list[i]->remote_addr = ntohll(cb->recv_buf.buf[i]);
	    cb->remote_chunk.chunk_list[i]->bitmap_g = (int *)kzalloc(sizeof(int) * BITMAP_INT_SIZE, GFP_KERNEL);
	    cb->remote_chunk.chunk_list[i]->replica_index = replica_index;
	    RDMABOX_bitmap_init(cb->remote_chunk.chunk_list[i]->bitmap_g);
	    //RDMABOX_session->free_chunk_index -= 1;
	    RDMABOX_session->mapping_swapspace_to_chunk[replica_index][select_chunk] = i;
	    cb->remote_chunk.chunk_map[i] = select_chunk;

	    cb->remote_chunk.chunk_size_g += 1;
	    cb->remote_chunk.c_state = C_READY;
	    atomic_set(cb->remote_chunk.remote_mapped + i, CHUNK_MAPPED);
	    atomic_set(RDMABOX_session->cb_index_map[replica_index] + (select_chunk), cb->cb_index);

	    chunk = cb->remote_chunk.chunk_list[i];
	    chunk->chunk_state = ACTIVE; 

#ifdef DEBUG
	    printk("rdmabox[%s]: replica_index:%d (chunk[%d] on node[%d]) is mapped to swapspace[%d]\n",__func__,replica_index,i,cb->cb_index,select_chunk);
#endif
	    break;
	}
    }
}


inline int RDMABOX_set_device_state(struct RDMABOX_file *xdev,
	enum RDMABOX_dev_state state)
{
    int ret = 0;

#ifdef DEBUG	
    printk("rdmabox[%s] set state=%d \n",__func__,state);
#endif

    spin_lock(&xdev->state_lock);
    switch (state) {
	case DEVICE_OPENNING:
#ifdef DEBUG	
	    printk("rdmabox[%s] DEVICE_OPENNING \n",__func__,state);

#endif
	    if (xdev->state == DEVICE_OFFLINE ||
		    xdev->state == DEVICE_RUNNING) {
		ret = -EINVAL;
		goto out;
	    }
	    xdev->state = state;
	    break;
	case DEVICE_RUNNING:
#ifdef DEBUG	
	    printk("rdmabox[%s] DEVICE_RUNNING \n",__func__,state);

#endif
	    xdev->state = state;
	    break;
	case DEVICE_OFFLINE:
#ifdef DEBUG	
	    printk("rdmabox[%s] DEVICE_OFFLINE \n",__func__,state);

#endif
	    xdev->state = state;
	    break;
	default:
	    printk("rdmabox[%s]:Unknown device state %d\n",__func__, state);

	    ret = -EINVAL;
    }
out:
    spin_unlock(&xdev->state_lock);
    return ret;
}

int rdma_read(struct RDMABOX_session *RDMABOX_sess, struct kernel_cb *cb, int cb_index, int chunk_index, struct remote_chunk_g *chunk, unsigned long offset, struct request *req, int cpuhint)
    //int rdma_read(struct RDMABOX_session *RDMABOX_sess, struct kernel_cb *cb, int cb_index, int chunk_index, struct remote_chunk_g *chunk, unsigned long offset, struct rdma_box *msg, int cpuhint)
{
    int ret;
    int err;
    int i;
    struct ib_send_wr *bad_wr;
    struct rdma_ctx *ctx;
    struct rdma_mr *mr;
    get_cpu(); 
    get_both(RDMABOX_sess, &ctx, &mr, cb->cb_index, cpuhint, 1, 1, 0);
    ctx->mr = mr;

    ctx->chunk_index = chunk_index; //chunk_index in cb
    ctx->cb = cb;
    ctx->reqlist[0] = req;
    ctx->numreq = 1;
    ctx->isSGE = 0;
    atomic_set(&ctx->in_flight, CTX_R_IN_FLIGHT);

    ctx->rdma_sgl_wr[0].addr = ctx->mr->rdma_dma_addr;
    //ctx->rdma_sgl_wr[0].lkey = cb->qp[0]->device->local_dma_lkey;
    ctx->rdma_sgl_wr[0].lkey = cb->qplist[cpuhint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
    ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.wr.sg_list->length = RDMABOX_PAGE_SIZE;
    ctx->rdma_sq_wr.wr.num_sge = 1;
#else
    ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.sg_list->length = RDMABOX_PAGE_SIZE;
    ctx->rdma_sq_wr.num_sge = 1;
#endif  
    ret = ib_post_send(cb->qplist[cpuhint], &ctx->rdma_sq_wr, &bad_wr);
    if (ret) {
	printk("rdmabox[%s]:client post read error ret=%d\n",__func__, ret);
	put_cpu();
	return ret;
    }

    put_cpu();
    return 0;
}

int sg_read(struct RDMABOX_session *RDMABOX_sess, struct kernel_cb *cb, int cb_index, int chunk_index, struct remote_chunk_g *chunk, unsigned long offset, struct request *req, int cpuhint)
    //int sg_read(struct RDMABOX_session *RDMABOX_sess, struct kernel_cb *cb, int cb_index, int chunk_index, struct remote_chunk_g *chunk, unsigned long offset, struct rdmabox_msg *msg, int cpuhint)
{
    int i;
    int ret;
    int err;
    struct ib_send_wr *bad_wr;
    struct scatterlist *sg;
    struct sg_table *sgt;
    int nents;
    struct rdma_ctx *ctx;
    //struct rdma_ctx *ctx = msg->ctx;
    get_cpu();
    get_single(RDMABOX_sess , &ctx, NULL, cb->cb_index, cpuhint,1, 0, 0);

    ctx->chunk_index = chunk_index; //chunk_index in cb
    ctx->cb = cb;
    ctx->reqlist[0] = req;
    ctx->numreq = 1;
    ctx->isSGE = 1;
    atomic_set(&ctx->in_flight, CTX_R_IN_FLIGHT);
    //ctx->RDMABOX_sess = RDMABOX_sess;

    sgt = &ctx->data_tbl;
    sgt->sgl = ctx->sgl;
    sg = sgt->sgl;

    sg_init_table(ctx->data_tbl.sgl, req->nr_phys_segments);
    ret = req_map_sg(req, ctx);
    if (ret) {
	printk("failed to map sg\n");
	put_cpu();
	return 1;
    }
    nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sgt->sgl, sgt->nents, DMA_FROM_DEVICE);
    if (!nents) {
	sg_mark_end(sg);
	sgt->nents = sgt->orig_nents;
	put_cpu();
	return 1;
    }
    //printk("rdmabox[%s] : mapped sgt->nents:%d nents:%d\n",__func__, sgt->nents,nents);
    for (i = 0; i < nents; i++) {
	ctx->rdma_sgl_wr[i].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
	ctx->rdma_sgl_wr[i].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
	//ctx->rdma_sgl_wr[i].lkey = cb->qp[0]->device->local_dma_lkey;
	ctx->rdma_sgl_wr[i].lkey = cb->qplist[cpuhint]->device->local_dma_lkey;
	sg = sg_next(sg);
    }

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
    ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.wr.sg_list = &ctx->rdma_sgl_wr[0];
    ctx->rdma_sq_wr.wr.num_sge = nents;
#else
    ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl_wr[0];
    ctx->rdma_sq_wr.num_sge = nents;
#endif	
    ret = ib_post_send(cb->qplist[cpuhint], &ctx->rdma_sq_wr, &bad_wr);
    if (ret) {
	printk("rdmabox[%s]:client post read error ret=%d\n",__func__, ret);
	put_cpu();
	return ret;
    }

    put_cpu();
    return 0;
}

struct kernel_cb* get_cb(struct RDMABOX_session *RDMABOX_session, int swapspace_index, struct kernel_cb **replica_cb_list)
{
    int i;
    int ret;
    int cb_index;

    spin_lock(&RDMABOX_session->cb_lock);
    cb_index = atomic_read(RDMABOX_session->cb_index_map[0] + swapspace_index);
    if (cb_index == NO_CB_MAPPED){
	//printk("rdmabox[%s]%s: NO_CB_MAPPED. \n", __func__,__LINE__);
	ret = RDMABOX_single_chunk_map(RDMABOX_session, swapspace_index, replica_cb_list);
	if(ret != NUM_REPLICA){
	    spin_unlock(&RDMABOX_session->cb_lock);
	    return NULL;
	}
    }else{
	replica_cb_list[0] = RDMABOX_session->cb_list[cb_index];
	for(i=1; i<NUM_REPLICA; i++){
	    cb_index = atomic_read(RDMABOX_session->cb_index_map[i] + swapspace_index);
	    replica_cb_list[i] = RDMABOX_session->cb_list[cb_index];
	}
    }
    spin_unlock(&RDMABOX_session->cb_lock);
out:
    // return primary cb
    return replica_cb_list[0];
}

int __rdma_write(struct RDMABOX_session *RDMABOX_sess, struct kernel_cb *cb,  int swapspace_index, unsigned long offset, int replica_index, struct rdma_ctx *ctx, int qphint)
{
    int ret;
    struct ib_send_wr *bad_wr;	
    int chunk_index;
    struct remote_chunk_g *chunk;

    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[replica_index][swapspace_index];
    chunk = cb->remote_chunk.chunk_list[chunk_index];

    ctx->offset = offset;
    ctx->cb = cb;
    ctx->chunk_ptr = chunk;
    ctx->chunk_index = chunk_index;
    ctx->rdma_sgl_wr[0].lkey = cb->qplist[qphint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
    ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
#else
    ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
#endif
    //printk("rdmabox[%s] post write swapspace_index:%d write_index:%d replica_index:%d cb_index:%d \n", __func__,swapspace_index,ctx->sending_item->start_index, replica_index,cb->cb_index);
    ret = ib_post_send(cb->qplist[qphint], &ctx->rdma_sq_wr, &bad_wr);
    if (ret) {
	printk("rdmabox[%s]: post write error. ret=%d\n",__func__, ret);
	return ret;
    }
    return 0;
}

int rdma_write(struct RDMABOX_session *RDMABOX_sess, int swapspace_index, unsigned long offset, struct local_page_list *tmp, int cpuhint)
{
    int i,j;
    int ret;
    struct rdma_ctx *ctx[NUM_REPLICA];
    struct rdma_mr *mr;
    unsigned char *cmem;
    struct timeval cur;
    struct kernel_cb *cb;
    struct remote_chunk_g *chunk;
    int chunk_index;
    struct kernel_cb *replica_cb_list[NUM_REPLICA];

    get_cpu();
    //find primary cb
    cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
    //printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
    if(!cb){
	printk("[%s]: cb is null \n", __func__);
	put_cpu();
	return 1;
    }

    //find primary chunk
    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
    chunk = cb->remote_chunk.chunk_list[chunk_index];

    // check if READONLY
    if(chunk->chunk_state >= READONLY){
	printk("rdmabox[%s]: this chunk is READONLY. MIGRATION on progress\n",__func__);
try_again:
	ret = put_sending_index(tmp);
	if(ret){
	    printk("[%s]: Fail to put sending index. retry \n", __func__);
	    goto try_again;
	}
	put_cpu();
	return 0;
    }

    //do_gettimeofday(&cur);

    atomic_set(&tmp->ref_count, 0);
    get_both(RDMABOX_sess, &ctx[0], &mr, replica_cb_list[0]->cb_index, cpuhint, 1, 1, 1);

    for(j=0; j<NUM_REPLICA; j++){
	if(j==0){
	    ctx[j]->mr = mr;
	    for (i=0; i < tmp->len;i++){
		struct tree_entry *entry;
		entry = tmp->batch_list[i];
		if(unlikely(!entry->page)){
		    printk("rdmabox[%s]: entry->page null i=%d, len=%d \n",__func__,i,tmp->len);
		    continue;
		}
		cmem = kmap_atomic(entry->page);
		memcpy(ctx[j]->mr->rdma_buf + (i*RDMABOX_PAGE_SIZE), cmem, RDMABOX_PAGE_SIZE);
		kunmap_atomic(cmem);
	    }
	}else{//j>0
	    get_single(RDMABOX_sess, &ctx[j], NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
	    ctx[j]->mr = ctx[0]->mr;
	}//else

	atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	ctx[j]->replica_index = j;
	ctx[j]->reqlist[0] = NULL;
	ctx[j]->numreq = 0;
	ctx[j]->isSGE = 0;
	ctx[j]->rdma_sgl_wr[0].addr = ctx[0]->mr->rdma_dma_addr;
	ctx[j]->prime_ctx = ctx[0];
	ctx[j]->len = tmp->len * RDMABOX_PAGE_SIZE;
	ctx[j]->sending_item = tmp;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	ctx[j]->rdma_sq_wr.wr.sg_list->length = ctx[j]->len;
	ctx[j]->rdma_sq_wr.wr.num_sge = 1;
#else
	ctx[j]->rdma_sq_wr.sg_list->length = ctx[j]->len;
	ctx[j]->rdma_sq_wr.num_sge = 1;
#endif
    }//replica forloop

#ifdef DISKBACKUP
    stackbd_bio_generate(ctx[0], tmp->cloned_bio, tmp->len*RDMABOX_PAGE_SIZE, tmp->start_index);
#endif

    // send out
    //ret = __rdma_write(RDMABOX_sess, replica_cb_list[0], swapspace_index, offset, 0, ctx[0], cpuhint%NUM_CHANNEL);
    ret = __rdma_write(RDMABOX_sess, replica_cb_list[0], swapspace_index, offset, 0, ctx[0], cpuhint);
    if (ret){ 
	put_cpu();
	return ret;
    }

    for(j=1; j<NUM_REPLICA; j++){
	//ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint%NUM_CHANNEL);
	ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint);
	if (ret){
	    put_cpu();
	    return ret;
	}
    }

    put_cpu();
    //update write ops
    //RDMABOX_session->wr_ops[swapspace_index][0] += 1;

    return 0;
}

// TODO : remove this if not using
uint32_t bitmap_value(int *bitmap)
{
    int i;
    uint32_t val = 1;
    for (i =0; i < BITMAP_INT_SIZE; i+=32) {
	if (bitmap[i] != 0){
	    val += 1;	
	}
    }	
    return val;
}
/*
   static int RDMABOX_send_activity(struct kernel_cb *cb)
   {
   int i,j;
   unsigned long wr_sum=0;
   unsigned long wr_sum_prev=0;
   unsigned long rd_sum=0;
   unsigned long rd_sum_prev=0;
   struct RDMABOX_session *RDMABOX_sess = cb->RDMABOX_sess;

   int chunk_index = cb->recv_buf.chunk_index;

// current window reset
wr_sum_prev = RDMABOX_sess->wr_ops[chunk_index][0];
rd_sum_prev = RDMABOX_sess->rd_ops[chunk_index][0];

msleep(1000);
cb->send_buf.type = RESP_ACTIVITY;

wr_sum = RDMABOX_sess->wr_ops[chunk_index][0];
rd_sum = RDMABOX_sess->rd_ops[chunk_index][0];

cb->send_buf.buf[0] = htonll(wr_sum + rd_sum);

post_send(cb);

printk("rdmabox[%s]: swapspace[%d] RD activity:%lu , WR activity:%lu \n",__func__,chunk_index, (rd_sum-rd_sum_prev), (wr_sum-wr_sum_prev));
return rd_sum ;
}
static int RDMABOX_check_activity(struct RDMABOX_session *RDMABOX_sess, int chunk_index)
{
int i,j;
unsigned long wr_sum=0;
unsigned long wr_sum_prev=0;
unsigned long rd_sum=0;
unsigned long rd_sum_prev=0;

//  for(j=0; j<3; j++){
//      wr_sum += RDMABOX_sess->wr_ops[chunk_index][j];
//      rd_sum += RDMABOX_sess->rd_ops[chunk_index][j];
//  }

// current window reset
wr_sum_prev = RDMABOX_sess->wr_ops[chunk_index][0];
rd_sum_prev = RDMABOX_sess->rd_ops[chunk_index][0];

msleep(1000);

wr_sum = RDMABOX_sess->wr_ops[chunk_index][0];
rd_sum = RDMABOX_sess->rd_ops[chunk_index][0];

printk("rdmabox[%s]: swapspace[%d] RD activity:%lu , WR activity:%lu \n",__func__,chunk_index, (rd_sum-rd_sum_prev), (wr_sum-wr_sum_prev));
return rd_sum ;
}
 */

static int reset_migration_info(struct kernel_cb *old_cb, struct kernel_cb *new_cb, int replica_index, int client_evict_chunk_index)
{
    int i;
    struct RDMABOX_session *RDMABOX_sess = old_cb->RDMABOX_sess;
#ifdef DEBUG
    printk("rdmabox[%s]: now reset migration information \n",__func__);
#endif

    for (i = 0; i < MAX_MR_SIZE_GB; i++) {
	if (old_cb->remote_chunk.migration_chunk_map[i] == 'm'){
	    old_cb->remote_chunk.chunk_list[i]->remote_rkey = 0;
	    old_cb->remote_chunk.chunk_list[i]->remote_addr = 0;
	    RDMABOX_bitmap_init(old_cb->remote_chunk.chunk_list[i]->bitmap_g);
	    //RDMABOX_sess->free_chunk_index += 1;
	    old_cb->remote_chunk.chunk_map[i] = -1;

	    old_cb->remote_chunk.chunk_size_g -= 1;
	    old_cb->remote_chunk.c_state = C_IDLE;
	    atomic_set(old_cb->remote_chunk.remote_mapped + i, CHUNK_UNMAPPED);

	    old_cb->remote_chunk.migration_chunk_map[i] = 0;
	    atomic_set(RDMABOX_sess->cb_index_map[replica_index] + client_evict_chunk_index, NO_CB_MAPPED);

#ifdef DEBUG
	    printk("rdmabox[%s]: ([%d]th chunk on server[%d]) is cleaned now.\n",__func__,i,old_cb->cb_index);
#endif
	}
	if(new_cb)
	    new_cb->remote_chunk.migration_chunk_map[i] = 0;
    }

    return 0;
}

static void set_chunk_state(struct kernel_cb *cb, int select_chunk, enum chunk_state state)
{
    int chunk_index;
    struct remote_chunk_g *chunk;
    struct RDMABOX_session *RDMABOX_sess = cb->RDMABOX_sess;

    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][select_chunk];
    chunk = cb->remote_chunk.chunk_list[chunk_index];
    chunk->chunk_state = state; 
#ifdef DEBUG
    printk("rdmabox[%s]: swapspace:%d, chunk_index:%d, chunk->chunk_state:%d  \n",__func__,select_chunk,chunk_index,chunk->chunk_state);
#endif

}

int hybrid_remote_read(struct RDMABOX_session *RDMABOX_sess,struct rdma_ctx *ctx, struct kernel_cb *cb, int chunk_index, struct remote_chunk_g *chunk, unsigned long offset, int swapspace_index, int qphint)
{
    int rst = 1;
    struct ib_send_wr *bad_wr;
    int i;
    int ret;
    struct request *req;
    struct scatterlist *sg;
    struct sg_table *sgt;
    int nents;

    //printk("rdmabox[%s] sge mapping numreq:%d\n",__func__,ctx->numreq);
    ctx->cb = cb;
    atomic_set(&ctx->in_flight, CTX_R_IN_FLIGHT);
    ctx->chunk_index = chunk_index; //chunk_index in cb

    if(ctx->isSGE){
	sgt = &ctx->data_tbl;
	sgt->sgl = ctx->sgl;
	sg = sgt->sgl;
	sg_init_table(ctx->data_tbl.sgl, ctx->numreq);
	ctx->data_tbl.nents = 0;
	for (i = 0; i < ctx->numreq; i++) {
	    req = ctx->reqlist[i];
	    ret = req_map_sg_each(req, ctx, sg);
	    if (ret) {
		printk("failed to map sg\n");
		return 1;
	    }
	    nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sg, 1, DMA_FROM_DEVICE);
	    if (!nents) {
		sg_mark_end(sg);
		sgt->nents = sgt->orig_nents;
		return 1;
	    }
	    //printk("rdmabox[%s] : mapped sgt->nents:%d nents:%d\n",__func__, sgt->nents,nents);

	    ctx->rdma_sgl_wr[i].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
	    //ctx->rdma_sgl_wr[i].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
	    ctx->rdma_sgl_wr[i].length = RDMABOX_PAGE_SIZE;
	    ctx->rdma_sgl_wr[i].lkey = cb->qplist[qphint]->device->local_dma_lkey;
	    sg_unmark_end(sg);
	    sg = sg_next(sg);
	}//for
	sg_mark_end(sg);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
	ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
	ctx->rdma_sq_wr.wr.num_sge = ctx->numreq;
	ctx->rdma_sq_wr.wr.sg_list = &ctx->rdma_sgl_wr[0];
#else
	ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
	ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
	ctx->rdma_sq_wr.num_sge = ctx->numreq;
	ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl_wr[0];
#endif	

    }else{
	ctx->isSGE = 0;
	ctx->rdma_sgl_wr[0].addr = ctx->mr->rdma_dma_addr;
	//ctx->rdma_sgl_wr[0].lkey = cb->qp[0]->device->local_dma_lkey;
	ctx->rdma_sgl_wr[0].lkey = cb->qplist[qphint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
	ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
	ctx->rdma_sq_wr.wr.sg_list->length = ctx->len;
	ctx->rdma_sq_wr.wr.num_sge = 1;
#else
	ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
	ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
	ctx->rdma_sq_wr.sg_list->length = ctx->len;
	ctx->rdma_sq_wr.num_sge = 1;
#endif	
    }

    rst = ib_post_send(cb->qplist[qphint], &ctx->rdma_sq_wr, &bad_wr);
    if (rst) {
	printk("rdmabox[%s]:client post read error rst=%d\n",__func__, rst);
	return rst;
    }

    //update read ops
    //RDMABOX_sess->rd_ops[swapspace_index][0] += 1;
    return 0;
}

int prep_dynMR_read(struct RDMABOX_session *RDMABOX_sess,struct rdma_ctx *ctx, struct kernel_cb *cb, int chunk_index, struct remote_chunk_g *chunk, int swapspace_index, int qphint)
{
    int rst = 1;
    struct ib_send_wr *bad_wr;
    int i;
    int ret;
    struct request *req;
    struct scatterlist *sg;
    struct sg_table *sgt;
    int nents;
    unsigned long offset;
    size_t start;

    //printk("rdmabox[%s] sge mapping numreq:%d\n",__func__,ctx->numreq);
    ctx->cb = cb;
    atomic_set(&ctx->in_flight, CTX_R_IN_FLIGHT);
    ctx->chunk_index = chunk_index; //chunk_index in cb

    ctx->isSGE = 1;
    sgt = &ctx->data_tbl;
    sgt->sgl = ctx->sgl;
    sg = sgt->sgl;
    sg_init_table(ctx->data_tbl.sgl, ctx->numreq);
    ctx->data_tbl.nents = 0;

    start = blk_rq_pos(ctx->reqlist[0]) << SECTOR_SHIFT;
    offset = start & ONE_GB_MASK;

    for (i = 0; i < ctx->numreq; i++) {
	req = ctx->reqlist[i];
	ret = req_map_sg_each(req, ctx, sg);
	if (ret) {
	    printk("failed to map sg\n");
	    return 1;
	}
	nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sg, 1, DMA_FROM_DEVICE);
	if (!nents) {
	    sg_mark_end(sg);
	    sgt->nents = sgt->orig_nents;
	    return 1;
	}
	//printk("rdmabox[%s] : mapped sgt->nents:%d nents:%d\n",__func__, sgt->nents,nents);

	ctx->rdma_sgl_wr[i].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
	//ctx->rdma_sgl_wr[i].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
	ctx->rdma_sgl_wr[i].length = RDMABOX_PAGE_SIZE;
	ctx->rdma_sgl_wr[i].lkey = cb->qplist[qphint]->device->local_dma_lkey;
	sg_unmark_end(sg);
	sg = sg_next(sg);
    }//for
    sg_mark_end(sg);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
    ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.wr.num_sge = ctx->numreq;
    ctx->rdma_sq_wr.wr.sg_list = &ctx->rdma_sgl_wr[0];
#else
    ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.num_sge = ctx->numreq;
    ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl_wr[0];
#endif	
/*
    rst = ib_post_send(cb->qplist[qphint], &ctx->rdma_sq_wr, &bad_wr);
    if (rst) {
	printk("rdmabox[%s]:client post read error rst=%d\n",__func__, rst);
	return rst;
    }
*/
    return 0;
}


int new_hybrid_batch_read_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
	int bitmap_i;
	struct request *tmp_first=NULL;
	int err, rst;
	int i,j;
	struct rdma_ctx *ctx[DOORBELL_BATCH_SIZE];
        int batch_cnt=0;
        int first_pos=0;
	int swapspace_index, swapspace_index_first;
	unsigned long start_idx_tmp, start_tmp;
	unsigned long start_idx_tmp_first, start_tmp_first;
	struct kernel_cb *cb, *cb_first;
	struct remote_chunk_g *chunk, *chunk_first;
	int chunk_index, chunk_index_first;
	struct kernel_cb *replica_cb_list[NUM_REPLICA];
	unsigned long offset;
	int cb_index;
	int must_go = 0;
        struct ib_send_wr * bad_wr;

#ifdef FLOW_CONTROL
retry_post:
	if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	    goto retry_post;
	}else{
	    atomic_inc(&num_outstanding);
	}
#endif

    get_cpu();
    while(batch_cnt < DOORBELL_BATCH_SIZE){
	struct request *tmp;
	err = get_read_req_item(&tmp);
	if(err){
	    if(tmp_first){	
		tmp = NULL;
		goto send;
	    }
	    put_cpu();
	    return 0;
	}

	start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT;
	start_tmp = start_idx_tmp << (SECTORS_PER_PAGE_SHIFT + SECTOR_SHIFT);
	swapspace_index = start_tmp >> ONE_GB_SHIFT;
	cb_index = atomic_read(RDMABOX_sess->cb_index_map[0] + swapspace_index);
	//printk("[%s]: Incoming Req:%x start_idx_tmp=%lu\n", __func__,tmp,start_idx_tmp);

	// for now, we don't consier replica. cb_index_map[0] is primary
	if(cb_index == NO_CB_MAPPED){
	    //printk("rdmabox[%s]: NO_CB_MAPPED. do nothing. start_idx_tmp:%lu cb_index:%d, swapspace_index:%d\n", __func__,start_idx_tmp,cb_index,swapspace_index);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
	    blk_mq_end_request(tmp, 0);
#else
	    blk_mq_end_io(tmp, 0);
#endif
            if(tmp_first){	
	        printk("rdmabox[%s]: NO_CB_MAPPED. but tmp_first exists. go send\n", __func__);
		tmp = NULL;
		goto send;
	    }
	    put_cpu();
	    return 0;
	}
	cb = RDMABOX_sess->cb_list[cb_index];

	chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	chunk = cb->remote_chunk.chunk_list[chunk_index];

        if(chunk->chunk_state == NONE_ALLOWED){
	    printk("rdmabox[%s]: this chunk is READONLY. MIGRATION on progress\n",__func__);
try_again:
	    err = put_read_req_item(tmp);
	    if(err){
		printk("rdmabox[%s]: Fail to put req item. retry \n", __func__);
		goto try_again;
	    }
	    continue;
	}

	offset = start_tmp & ONE_GB_MASK;	
	bitmap_i = (int)(offset / RDMABOX_PAGE_SIZE);

	// check if it is in remote
	rst = RDMABOX_bitmap_test(chunk->bitmap_g, bitmap_i);
	if(!rst){
	    //printk("rdmabox[%s]%s: No remote mapping. Go check local \n", __func__,__LINE__);
	    err = radix_read(RDMABOX_sess,tmp);
	    if(err){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
		blk_mq_end_request(tmp, 0);
#else
		blk_mq_end_io(tmp, 0);
#endif
	    }
            if(tmp_first){	
	        printk("rdmabox[%s]: no remote mapping. but tmp_first exists. go send\n", __func__);
		tmp = NULL;
		goto send;
	    }
	    put_cpu();
	    return 0;
	}

        if (tmp_first && swapspace_index_first != swapspace_index) {
            goto send;
        }

	if(!tmp_first)
	{
reload:
            tmp_first=tmp;
            cb_first = cb;
            swapspace_index_first = swapspace_index;
            chunk_index_first = chunk_index;
            chunk_first = chunk;

            start_idx_tmp_first = blk_rq_pos(tmp_first) >> SECTORS_PER_PAGE_SHIFT;

	    first_pos = batch_cnt;
            get_single(RDMABOX_sess , &ctx[first_pos], NULL, cb_first->cb_index, cpuhint,1, 0, 0);
            ctx[first_pos]->reqlist[ctx[first_pos]->numreq++] = tmp_first;
            ctx[first_pos]->len = RDMABOX_PAGE_SIZE;
	    batch_cnt++;

            if(must_go){
	        tmp = NULL;
	        goto send;
            }else{
	        continue;
            }
        }

        // check if there is contiguous item
        if( (start_idx_tmp == (start_idx_tmp_first + ctx[first_pos]->numreq) ) &&
 	    ((ctx[first_pos]->numreq + 1) <= PAGE_CLUSTER) )
        {
            ctx[first_pos]->reqlist[ctx[first_pos]->numreq++] = tmp;
            ctx[first_pos]->len = ctx[first_pos]->len + RDMABOX_PAGE_SIZE;
            //printk("rdmabox[%s]: merge start_idx_tmp=%lu numreq:%d start_tmp=%lu \n",__func__,start_idx_tmp,ctx[first_pos]->numreq, start_tmp);
            continue;
        }
	 // if gets here, fail to preMR or dynMR batch, now we do doorbell batch
         get_single(RDMABOX_sess, &ctx[batch_cnt], NULL, cb_first->cb_index, cpuhint, 1, 0, 0);

         ctx[batch_cnt]->numreq = 0;
         atomic_set(&ctx[batch_cnt]->ref_count,0);
         //ctx[batch_cnt]->total_segments = tmp->nr_phys_segments;
         ctx[batch_cnt]->reqlist[ctx[batch_cnt]->numreq++] = tmp;
         ctx[batch_cnt]->len = RDMABOX_PAGE_SIZE;
         batch_cnt++;
/*
         printk("rdmabox[%s]: doorbell merge req:%p ctx[%d]=%p ctx_first[0]=%p\n", __func__,tmp, batch_cnt-1, ctx[batch_cnt-1], ctx[0]);
         for(i=0; i<batch_cnt; i++){
             printk("rdmabox[%s]: batch_cnt:%d/%d req:%p numreq:%d ctx[%d]=%p \n", __func__,i,batch_cnt,ctx[i]->reqlist[0],ctx[i]->numreq,i, ctx[i]);
         }
*/
         continue;

send:
        if(batch_cnt == 1){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx[0]->rdma_sq_wr.wr.next = NULL;
#else
            ctx[0]->rdma_sq_wr.next = NULL;
#endif
            //printk("rdmabox[%s]: single dynMR prep read. req:%p numreq:%d swapspace:%lu \n", __func__,ctx[0]->reqlist[0], ctx[0]->numreq, swapspace_index_first);
            prep_dynMR_read(RDMABOX_sess, ctx[0], cb_first, chunk_index_first, chunk_first, swapspace_index_first, cpuhint);
        }else{
            for(i=0; i<batch_cnt; i++){
               //printk("rdmabox[%s]: batch_cnt:%d/%d dynMR prep read. req:%p numreq:%d ctx[%d]=%p \n", __func__,i,batch_cnt,ctx[i]->reqlist[0],ctx[i]->numreq,batch_cnt, ctx[i]);
               prep_dynMR_read(RDMABOX_sess, ctx[i], cb_first, chunk_index_first, chunk_first, swapspace_index_first, cpuhint);

               //chaining
               if(i == batch_cnt - 1){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	           ctx[i]->rdma_sq_wr.wr.next = NULL;
#else
                   ctx[i]->rdma_sq_wr.next = NULL;
#endif
               }else{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
                   ctx[i]->rdma_sq_wr.wr.next = &ctx[i+1]->rdma_sq_wr;
#else
                   ctx[i]->rdma_sq_wr.next = &ctx[i+1]->rdma_sq_wr;
#endif
               }
            }
        } // batch_cnt

        err = ib_post_send(cb_first->qplist[cpuhint], &ctx[0]->rdma_sq_wr, &bad_wr);
        if (err) {
	    printk("rdmabox[%s]:client post read error rst=%d\n",__func__, rst);
    	    return 1;
        }
        batch_cnt=0;

        if(!tmp){
            //must_go = 0;
            put_cpu();
            return 0;
        }else{
            must_go = 1;
            goto reload;
        }

    }//while 

    return 0;
}

int doorbell_remote_read_sge(struct RDMABOX_session *RDMABOX_sess,struct rdma_ctx **ctx, struct kernel_cb *cb, int chunk_index, struct remote_chunk_g *chunk, unsigned long offset, int swapspace_index, int qphint)
{
   int i,k;
   int rst = 0;
   struct ib_send_wr *bad_wr;
   struct request *req;
   size_t start;
   struct scatterlist *sg;
   struct sg_table *sgt;
   int nents;

   for(i=0; i<ctx[0]->numreq; i++){
     //printk("rdmabox[%s]: ctx[%d] process. total:%d\n", __func__,i,ctx[0]->numreq);
     req = ctx[i]->reqlist[0];

     start = blk_rq_pos(req) << SECTOR_SHIFT;
     offset = start & ONE_GB_MASK;

     sgt = &ctx[i]->data_tbl;
     sgt->sgl = ctx[i]->sgl;
     sg = sgt->sgl;

     sg_init_table(ctx[i]->data_tbl.sgl, req->nr_phys_segments);
     rst = req_map_sg(req, ctx[i]);
     if (rst) {
	printk("failed to map sg\n");
	put_cpu();
	return 1;
      }
      nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sgt->sgl, sgt->nents, DMA_FROM_DEVICE);
      if (!nents) {
	sg_mark_end(sg);
	sgt->nents = sgt->orig_nents;
	put_cpu();
	return 1;
      }
      //printk("rdmabox[%s] : mapped sgt->nents:%d nents:%d\n",__func__, sgt->nents,nents);
      for (k = 0; k < nents; k++) {
	ctx[i]->rdma_sgl_wr[k].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
	ctx[i]->rdma_sgl_wr[k].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
	ctx[i]->rdma_sgl_wr[k].lkey = cb->qplist[qphint]->device->local_dma_lkey;
	sg = sg_next(sg);
      }

      atomic_set(&ctx[i]->in_flight, CTX_R_IN_FLIGHT);

      if(i>0)
          ctx[i]->numreq = 1;

      ctx[i]->isSGE = 1;
      ctx[i]->offset = offset;
      ctx[i]->cb = cb;
      ctx[i]->chunk_ptr = chunk;
      ctx[i]->chunk_index = chunk_index;
      ctx[i]->rdma_sgl_wr[0].lkey = cb->qplist[qphint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
      ctx[i]->rdma_sq_wr.rkey = chunk->remote_rkey;
      ctx[i]->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
      ctx[i]->rdma_sq_wr.wr.num_sge = 1;
      //ctx[i]->rdma_sq_wr.wr.sg_list->length = ctx[i]->len;

      if(i == ctx[0]->numreq - 1){
	//printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is null\n", __func__,i,ctx[0]->numreq);
	ctx[i]->rdma_sq_wr.wr.next = NULL; 
      }else{
	ctx[i]->rdma_sq_wr.wr.next = &ctx[i+1]->rdma_sq_wr; 
      }
#else
      ctx[i]->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
      ctx[i]->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
      ctx[i]->rdma_sq_wr.num_sge = 1;
      if(i == ctx[0]->numreq - 1){
	//printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is null\n", __func__,i,ctx[0]->numreq);
	ctx[i]->rdma_sq_wr.next = NULL; 
      }else{
	//printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is ctx[%d]\n", __func__,i,ctx[0]->numreq,i+1);
	ctx[i]->rdma_sq_wr.next = &ctx[i+1]->rdma_sq_wr; 
      }
#endif
  }
  ctx[0]->numreq = 1;

  rst = ib_post_send(cb->qplist[qphint], &ctx[0]->rdma_sq_wr, &bad_wr);
  if (rst) {
    printk("rdmabox[%s]:client post read error rst=%d\n",__func__, rst);
    return rst;
  }

  return 0;
}


int doorbell_remote_read(struct RDMABOX_session *RDMABOX_sess,struct rdma_ctx **ctx, struct kernel_cb *cb, int chunk_index, struct remote_chunk_g *chunk, unsigned long offset, int swapspace_index, int qphint)
{
    int i;
    int rst = 0;
    struct ib_send_wr *bad_wr;
    struct request *req;
    size_t start;

    for(i=0; i<ctx[0]->numreq; i++){
	//printk("rdmabox[%s]: ctx[%d] process. total:%d\n", __func__,i,ctx[0]->numreq);
	req = ctx[i]->reqlist[0];

	start = blk_rq_pos(req) << SECTOR_SHIFT;
	offset = start & ONE_GB_MASK;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	ctx[i]->rdma_sq_wr.wr.sg_list = &ctx[i]->rdma_sgl_wr[0];
	if(i == ctx[0]->numreq - 1){
	    //printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is null\n", __func__,i,ctx[1]->numreq);
	    ctx[i]->rdma_sq_wr.wr.next = NULL; 
	}else{
	    //printk("rdmabox[%s]: ctx[%d] total:%d. connect rdma_sq_wr.next %d \n", __func__,i,ctx[0]->numreq, i+1);
	    ctx[i]->rdma_sq_wr.wr.next = &ctx[i+1]->rdma_sq_wr; 
	}
#else
	ctx[i]->rdma_sq_wr.sg_list = &ctx[i]->rdma_sgl_wr[0];
	if(i == ctx[0]->numreq - 1){
	    //printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is null\n", __func__,i,ctx[0]->numreq);
	    ctx[i]->rdma_sq_wr.next = NULL; 
	}else{
	    //printk("rdmabox[%s]: ctx[%d] total:%d. connect rdma_sq_wr.next %d \n", __func__,i,ctx[0]->numreq, i+1);
	    ctx[i]->rdma_sq_wr.next = &ctx[i+1]->rdma_sq_wr; 
	}
#endif

	atomic_set(&ctx[i]->in_flight, CTX_R_IN_FLIGHT);

	if(i>0)
	    ctx[i]->numreq = 1;

	ctx[i]->isSGE = 0;

	//chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	//chunk = cb->remote_chunk.chunk_list[chunk_index];

	ctx[i]->offset = offset;
	ctx[i]->cb = cb;
	ctx[i]->chunk_ptr = chunk;
	ctx[i]->chunk_index = chunk_index;
	ctx[i]->rdma_sgl_wr[0].lkey = cb->qplist[qphint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	ctx[i]->rdma_sq_wr.rkey = chunk->remote_rkey;
	ctx[i]->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
	ctx[i]->rdma_sq_wr.wr.num_sge = 1;
	ctx[i]->rdma_sq_wr.wr.sg_list->length = ctx[i]->len;
#else
	ctx[i]->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
	ctx[i]->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
	ctx[i]->rdma_sq_wr.num_sge = 1;
	ctx[i]->rdma_sq_wr.sg_list->length = ctx[i]->len;
#endif
	ctx[i]->rdma_sgl_wr[0].addr = ctx[i]->mr->rdma_dma_addr;
    }
    ctx[0]->numreq = 1;

    rst = ib_post_send(cb->qplist[qphint], &ctx[0]->rdma_sq_wr, &bad_wr);
    if (rst) {
	printk("rdmabox[%s]:client post read error rst=%d\n",__func__, rst);
	return rst;
    }

    return 0;
}


int remote_read(struct RDMABOX_session *RDMABOX_sess,struct rdma_ctx *ctx, struct kernel_cb *cb, int chunk_index, struct remote_chunk_g *chunk, unsigned long offset, int swapspace_index, int qphint)
{
    int rst = 1;
    struct ib_send_wr *bad_wr;
    int i;
    int ret;
    struct request *req;
#if defined(USE_SGE_READ) || defined(SUPPORT_SGE_READ_BATCH)
    struct scatterlist *sg;
    struct sg_table *sgt;
    int nents;
#endif

    //printk("rdmabox[%s] sge mapping numreq:%d\n",__func__,ctx->numreq);
    ctx->cb = cb;
    atomic_set(&ctx->in_flight, CTX_R_IN_FLIGHT);
    ctx->chunk_index = chunk_index; //chunk_index in cb

#if defined(USE_SGE_READ) || defined(SUPPORT_SGE_READ_BATCH)
    ctx->isSGE = 1;
    sgt = &ctx->data_tbl;
    sgt->sgl = ctx->sgl;
    sg = sgt->sgl;
    sg_init_table(ctx->data_tbl.sgl, ctx->numreq);
    ctx->data_tbl.nents = 0;

    for (i = 0; i < ctx->numreq; i++) {
	req = ctx->reqlist[i];
	ret = req_map_sg_each(req, ctx, sg);
	if (ret) {
	    printk("failed to map sg\n");
	    return 1;
	}
	nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sg, 1, DMA_FROM_DEVICE);
	if (!nents) {
	    sg_mark_end(sg);
	    sgt->nents = sgt->orig_nents;
	    return 1;
	}
	//printk("rdmabox[%s] : mapped sgt->nents:%d nents:%d\n",__func__, sgt->nents,nents);

	ctx->rdma_sgl_wr[i].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
	//ctx->rdma_sgl_wr[i].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
	ctx->rdma_sgl_wr[i].length = RDMABOX_PAGE_SIZE;
	ctx->rdma_sgl_wr[i].lkey = cb->qplist[qphint]->device->local_dma_lkey;
	sg_unmark_end(sg);
	sg = sg_next(sg);
    }//for
    sg_mark_end(sg);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
    ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.wr.num_sge = ctx->numreq;
    ctx->rdma_sq_wr.wr.sg_list = &ctx->rdma_sgl_wr[0];
#else
    ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.num_sge = ctx->numreq;
    ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl_wr[0];
#endif	

#else // USE_SGE_READ SUPPORT_SGE_READ_BATCH
    ctx->isSGE = 0;
    ctx->rdma_sgl_wr[0].addr = ctx->mr->rdma_dma_addr;
    ctx->rdma_sgl_wr[0].lkey = cb->qplist[qphint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
    ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.wr.sg_list->length = ctx->len;
    ctx->rdma_sq_wr.wr.num_sge = 1;
#else
    ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
    ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
    ctx->rdma_sq_wr.sg_list->length = ctx->len;
    ctx->rdma_sq_wr.num_sge = 1;
#endif	

#endif // USE_SGE_READ SUPPORT_SGE_READ_BATCH

    rst = ib_post_send(cb->qplist[qphint], &ctx->rdma_sq_wr, &bad_wr);
    if (rst) {
	printk("rdmabox[%s]:client post read error rst=%d\n",__func__, rst);
	return rst;
    }

    //update read ops
    //RDMABOX_sess->rd_ops[swapspace_index][0] += 1;
    return 0;
}

int request_read(struct RDMABOX_session *RDMABOX_sess, struct request *req, int cpuhint)
    //int request_read(struct RDMABOX_session *RDMABOX_sess, struct rdmabox_msg * msg, int cpuhint)
{
    int i;
    int retval = 0;
    int rst = 1;
    int swapspace_index;
    int replica_index;
    unsigned long chunk_offset;	
    struct kernel_cb *cb;
    int cb_index;
    int chunk_index;
    struct remote_chunk_g *chunk;
    int bitmap_i;
    unsigned long start = blk_rq_pos(req) << SECTOR_SHIFT;

    //unsigned long start_idx = blk_rq_pos(req) >> SECTORS_PER_PAGE_SHIFT;
    //printk("rdmabox[%s][%d]: start_idx=%d\n", __func__,cpuhint,start_idx);
    /*
       if(atomic_read(&RDMABOX_sess->rdma_on) != DEV_RDMA_ON){
    //printk("rdmabox[%s]: RDMA network is not connected. \n", __func__);
    return 1;
    }
     */
#ifdef FLOW_CONTROL
retry_post:
    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	goto retry_post;
    }else{
	atomic_inc(&num_outstanding);
#endif

	swapspace_index = start >> ONE_GB_SHIFT;
	chunk_offset = start & ONE_GB_MASK;	
	bitmap_i = (int)(chunk_offset / RDMABOX_PAGE_SIZE);

	for(i=0; i<NUM_REPLICA; i++){
	    replica_index = i;
	    cb_index = atomic_read(RDMABOX_sess->cb_index_map[i] + swapspace_index);
	    if(cb_index == NO_CB_MAPPED){
		//printk("rdmabox[%s]: NO_CB_MAPPED. do nothing. replica_index:%d cb_index:%d, swapspace_index:%d\n", __func__,replica_index,cb_index,swapspace_index);
		rst = 1;
		continue;
	    }
	    cb = RDMABOX_sess->cb_list[cb_index];

	    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[replica_index][swapspace_index];
	    chunk = cb->remote_chunk.chunk_list[chunk_index];

	    rst = RDMABOX_bitmap_test(chunk->bitmap_g, bitmap_i);
	    if (rst){ //remote recorded
#ifdef USE_SGE_READ
		retval = sg_read(RDMABOX_sess, cb, cb_index, chunk_index, chunk, chunk_offset, req, cpuhint); 
		//retval = sg_read(RDMABOX_sess, cb, cb_index, chunk_index, chunk, chunk_offset, msg, cpuhint); 
#else
		retval = rdma_read(RDMABOX_sess, cb, cb_index, chunk_index, chunk, chunk_offset, req, cpuhint); 
		//retval = rdma_read(RDMABOX_sess, cb, cb_index, chunk_index, chunk, chunk_offset, msg, cpuhint); 
#endif
		if (unlikely(retval)) {
		    printk("rdmabox[%s]: failed to read from remote\n",__func__);
		    rst = 1;
		    continue;
		}
		//update read ops
		//RDMABOX_sess->rd_ops[swapspace_index][RDMABOX_sess->pos_rd_hist] += 1;
		//RDMABOX_sess->rd_ops[swapspace_index][0] += 1;
		return 0;
	    }else{
		//printk("rdmabox[%s]: no remote record. do nothing. replica_index:%d cb_index:%d swapspace_index:%d chunk_index:%d bitmap_i:%d\n", __func__,replica_index,cb_index,swapspace_index,chunk_index,bitmap_i);
		rst = 1;
	    }

	}//for

	return rst;
    }

#ifdef DISKBACKUP
    void stackbd_bio_generate_req(struct rdma_ctx *ctx, struct request *tmp)
    {
	struct bio *cloned_bio = NULL;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	cloned_bio = bio_clone_fast(tmp->bio, GFP_ATOMIC, io_bio_set);
#else
	cloned_bio = bio_clone(tmp->bio, GFP_ATOMIC); 
#endif
	cloned_bio->bi_private = uint64_from_ptr(ctx);
	stackbd_make_request5(cloned_bio);
    }

    void stackbd_bio_generate_mr(struct rdma_ctx *ctx, struct request *tmp)
    {

	struct bio *cloned_bio = NULL;
	struct page *pg = NULL;
	unsigned int nr_segs = tmp->nr_phys_segments;
	unsigned int io_size = nr_segs * RDMABOX_PAGE_SIZE;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	cloned_bio = bio_clone_fast(tmp->bio, GFP_ATOMIC, io_bio_set);
#else
	cloned_bio = bio_clone(tmp->bio, GFP_ATOMIC); 
#endif
	pg = virt_to_page(ctx->mr->rdma_buf);
	cloned_bio->bi_io_vec->bv_page  = pg; 
	cloned_bio->bi_io_vec->bv_len = io_size;
	cloned_bio->bi_io_vec->bv_offset = 0;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 14, 0)
	cloned_bio->bi_iter.bi_size = io_size;
#else
	cloned_bio->bi_size = io_size;
#endif
	cloned_bio->bi_private = uint64_from_ptr(ctx);
	stackbd_make_request5(cloned_bio);
    }

    void stackbd_bio_generate(struct rdma_ctx *ctx, struct bio *cloned_bio, unsigned int io_size, size_t start_index)
    {
	//struct bio *cloned_bio = tmp->cloned_bio;
	struct page *pg = NULL;
	//unsigned int nr_segs = tmp->len;
	//unsigned int io_size = tmp->len * RDMABOX_PAGE_SIZE;
	//unsigned int io_size = nr_segs * RDMABOX_PAGE_SIZE;

	pg = virt_to_page(ctx->mr->rdma_buf);
	//cloned_bio->bi_sector = tmp->start_index  << SECTORS_PER_PAGE_SHIFT;
	cloned_bio->bi_sector = start_index  << SECTORS_PER_PAGE_SHIFT;
	cloned_bio->bi_io_vec->bv_page = pg; 
	cloned_bio->bi_io_vec->bv_len = io_size;
	cloned_bio->bi_io_vec->bv_offset = 0;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 14, 0)
	cloned_bio->bi_iter.bi_size = io_size;
#else
	cloned_bio->bi_size = io_size;
#endif
	cloned_bio->bi_private = uint64_from_ptr(ctx);
	stackbd_make_request5(cloned_bio);
    }

    void stackbd_bio_generate_batch(struct rdma_ctx *ctx, size_t start_index)
    {
	struct bio *cloned_bio = NULL;
	struct page *pg = NULL;
	//unsigned int nr_segs = ctx->len;
	//unsigned int io_size = ctx->len * RDMABOX_PAGE_SIZE;
	unsigned int io_size = ctx->len;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	cloned_bio = bio_clone_fast(ctx->reqlist[0], GFP_ATOMIC, io_bio_set);
#else
	cloned_bio = bio_clone(ctx->reqlist[0]->bio, GFP_ATOMIC); 
#endif
	pg = virt_to_page(ctx->mr->rdma_buf);
	cloned_bio->bi_sector = start_index  << SECTORS_PER_PAGE_SHIFT;
	cloned_bio->bi_io_vec->bv_page = pg; 
	cloned_bio->bi_io_vec->bv_len = io_size;
	cloned_bio->bi_io_vec->bv_offset = 0;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 14, 0)
	cloned_bio->bi_iter.bi_size = io_size;
#else
	cloned_bio->bi_size = io_size;
#endif
	cloned_bio->bi_private = uint64_from_ptr(ctx);
	stackbd_make_request5(cloned_bio);
    }

#endif // DISKBACKUP
/*
int prep_dynMR_write(struct RDMABOX_session *RDMABOX_sess, struct rdma_ctx **ctx, struct kernel_cb **replica_cb_list, int swapspace_index, int cpuhint, unsigned long start_idx_tmp_first, int debug)
{
    int retval = 0;
    int i,j,k;
    int ret;
    struct ib_send_wr * bad_wr;
    struct request *req;
    struct scatterlist *sg;
    struct sg_table *sgt;
    int nents;
    int sg_pos;
    int chunk_index;
    struct remote_chunk_g *chunk;
    unsigned long offset;
    size_t start;

    sgt = &ctx[0]->data_tbl;
    sgt->sgl = ctx[0]->sgl;
    sg = sgt->sgl;

    sg_init_table( ctx[0]->data_tbl.sgl, ctx[0]->total_segments);
    ctx[0]->data_tbl.nents = 0;
    sg_pos=0;

    for (i = 0; i < ctx[0]->numreq; i++) {
        req = ctx[0]->reqlist[i];

	start = blk_rq_pos(req) << SECTOR_SHIFT;
        offset = start & ONE_GB_MASK;

        ret = req_map_sg_each(req, ctx[0], sg);
        if (ret) {
            printk("failed to map sg\n");
            return 1;
        }
        nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sg, sgt->nents, DMA_TO_DEVICE);
        if (nents == 0) {
            printk("rdmabox[%s] : !nents req->segs:%d mapped sgt->nents:%d nents:%d\n",__func__,req->nr_phys_segments, sgt->nents, nents);
            sg_mark_end(sg);
            sgt->nents = sgt->orig_nents;
            return 1;
        }
	if(debug)
            printk("rdmabox[%s] : req:%p req->segs:%d totalseg:%d mapped sgt->nents:%d nents:%d\n",__func__,req, req->nr_phys_segments,ctx[0]->total_segments, sgt->nents, nents);

        for (k=sg_pos; k < (sg_pos+nents); k++) {
	    if(debug)
                printk("rdmabox[%s] : req:%p process sg[%d] nents:%d\n",__func__,req, k, nents);
            ctx[0]->rdma_sgl_wr[k].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
            ctx[0]->rdma_sgl_wr[k].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
            ctx[0]->rdma_sgl_wr[k].lkey = replica_cb_list[0]->qplist[cpuhint]->device->local_dma_lkey;
            sg_unmark_end(sg);
            sg = sg_next(sg);
        }
	sg_pos += nents;
    }//for
    sg_mark_end(sg);

    for(j=0; j<NUM_REPLICA; j++){
	   if(j>0){
		get_single(RDMABOX_sess, &ctx[j], NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
		ctx[j]->len = ctx[0]->len;
	   }//j>0
           atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	   ctx[j]->sending_item = NULL;
	   ctx[j]->replica_index = j;
	   ctx[j]->isSGE = 1;
	   ctx[j]->prime_ctx = ctx[0];

           chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[j][swapspace_index];
           chunk = replica_cb_list[j]->remote_chunk.chunk_list[chunk_index];
           ctx[j]->offset = offset;
           ctx[j]->cb = replica_cb_list[j];
           ctx[j]->chunk_ptr = chunk;
           ctx[j]->chunk_index = chunk_index;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
           ctx[j]->rdma_sq_wr.rkey = chunk->remote_rkey;
           ctx[j]->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
           //ctx[j]->rdma_sq_wr.wr.num_sge = sg_pos;
           ctx[j]->rdma_sq_wr.wr.num_sge = sgt->nents;
           ctx[j]->rdma_sq_wr.wr.sg_list = &ctx[0]->rdma_sgl_wr[0];
#else
           ctx[j]->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
           ctx[j]->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
           //ctx[j]->rdma_sq_wr.num_sge = sg_pos;
           ctx[j]->rdma_sq_wr.num_sge = sgt->nents;
           ctx[j]->rdma_sq_wr.sg_list = &ctx[0]->rdma_sgl_wr[0];
#endif
    }//replica forloop

#ifdef DISKBACKUP
	struct bio *cloned_bio;
	cloned_bio = create_bio_copy(ctx[0]->reqlist[0]->bio);
	stackbd_bio_generate(ctx[0], cloned_bio, ctx[0]->len, start_idx_tmp_first );
	//stackbd_bio_generate_batch(ctx[0], start_idx_tmp_first);
#endif
	return 0;
}
*/

int prep_dynMR_write(struct RDMABOX_session *RDMABOX_sess, struct rdma_ctx *ctx, struct kernel_cb **replica_cb_list, int swapspace_index, int cpuhint, unsigned long start_idx_tmp_first, int debug)
{
    int retval = 0;
    int i,j,k;
    int ret;
    struct ib_send_wr * bad_wr;
    struct request *req;
    struct scatterlist *sg;
    struct sg_table *sgt;
    int nents;
    int sg_pos;
    int chunk_index;
    struct remote_chunk_g *chunk;
    unsigned long offset;
    size_t start;

    sgt = &ctx->data_tbl;
    sgt->sgl = ctx->sgl;
    sg = sgt->sgl;

    sg_init_table( ctx->data_tbl.sgl, ctx->total_segments);
    ctx->data_tbl.nents = 0;
    sg_pos=0;

    for (i = 0; i < ctx->numreq; i++) {
        req = ctx->reqlist[i];

	start = blk_rq_pos(req) << SECTOR_SHIFT;
        offset = start & ONE_GB_MASK;

        ret = req_map_sg_each(req, ctx, sg);
        if (ret) {
            printk("failed to map sg\n");
            return 1;
        }
        nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sg, sgt->nents, DMA_TO_DEVICE);
        if (nents == 0) {
            printk("rdmabox[%s] : !nents req->segs:%d mapped sgt->nents:%d nents:%d\n",__func__,req->nr_phys_segments, sgt->nents, nents);
            sg_mark_end(sg);
            sgt->nents = sgt->orig_nents;
            return 1;
        }
	if(debug)
            printk("rdmabox[%s] : req:%p req->segs:%d totalseg:%d mapped sgt->nents:%d nents:%d\n",__func__,req, req->nr_phys_segments,ctx->total_segments, sgt->nents, nents);

        for (k=sg_pos; k < (sg_pos+nents); k++) {
	    if(debug)
                printk("rdmabox[%s] : req:%p process sg[%d] nents:%d\n",__func__,req, k, nents);
            ctx->rdma_sgl_wr[k].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
            ctx->rdma_sgl_wr[k].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
            ctx->rdma_sgl_wr[k].lkey = replica_cb_list[0]->qplist[cpuhint]->device->local_dma_lkey;
            sg_unmark_end(sg);
            sg = sg_next(sg);
        }
	sg_pos += nents;
    }//for
    sg_mark_end(sg);

    for(j=0; j<NUM_REPLICA; j++){
	   if(j>0){
		get_single(RDMABOX_sess, &ctx, NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
		ctx->len = ctx->len;
	   }//j>0
           atomic_set(&ctx->in_flight, CTX_W_IN_FLIGHT);
	   ctx->sending_item = NULL;
	   ctx->replica_index = j;
	   ctx->isSGE = 1;
	   ctx->prime_ctx = ctx;

           chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[j][swapspace_index];
           chunk = replica_cb_list[j]->remote_chunk.chunk_list[chunk_index];
           ctx->offset = offset;
           ctx->cb = replica_cb_list[j];
           ctx->chunk_ptr = chunk;
           ctx->chunk_index = chunk_index;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
           ctx->rdma_sq_wr.rkey = chunk->remote_rkey;
           ctx->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
           //ctx->rdma_sq_wr.wr.num_sge = sg_pos;
           ctx->rdma_sq_wr.wr.num_sge = sgt->nents;
           ctx->rdma_sq_wr.wr.sg_list = &ctx->rdma_sgl_wr[0];
#else
           ctx->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
           ctx->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
           //ctx->rdma_sq_wr.num_sge = sg_pos;
           ctx->rdma_sq_wr.num_sge = sgt->nents;
           ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl_wr[0];
#endif
    }//replica forloop

#ifdef DISKBACKUP
	struct bio *cloned_bio;
	cloned_bio = create_bio_copy(ctx->reqlist[0]->bio);
	stackbd_bio_generate(ctx, cloned_bio, ctx->len, start_idx_tmp_first );
	//stackbd_bio_generate_batch(ctx, start_idx_tmp_first);
#endif

	return 0;
}


//single IO write sge
int request_write_sge(struct RDMABOX_session *RDMABOX_sess, struct request *req, int cpuhint)
{
	    int retval = 0;
	    int swapspace_index;
	    int boundary_index;
	    unsigned long offset;        
	    int i,j,k;
	    int ret;
	    struct rdma_ctx *ctx[NUM_REPLICA];
	    struct kernel_cb *cb;
	    struct kernel_cb *replica_cb_list[NUM_REPLICA];
	    struct bio *tmpbio = req->bio;
	    unsigned int nr_seg = req->nr_phys_segments;

	    struct scatterlist *sg;
	    struct sg_table *sgt;
	    int nents;
	    size_t start = blk_rq_pos(req) << SECTOR_SHIFT;

	    //unsigned long start_idx = blk_rq_pos(req) >> SECTORS_PER_PAGE_SHIFT;
	    //printk("rdmabox[%s][%d]: start_idx=%d\n", __func__,cpuhint,start_idx);

#ifdef FLOW_CONTROL
retry_post:
	    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
		goto retry_post;
	    }else{
		atomic_inc(&num_outstanding);
#endif

		swapspace_index = start >> ONE_GB_SHIFT;
		boundary_index = (start + (nr_seg*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;
		offset = start & ONE_GB_MASK;        

		// check two chunk
		if (swapspace_index != boundary_index) {
		    printk("rdmabox[%s]%s: two chunk. \n", __func__,__LINE__);
		    radix_write(RDMABOX_sess,req);
		    return 0;
		}//two chunk

		get_cpu();
		//find primary cb
		cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
		//printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
		if(!cb){
		    printk("[%s]: cb is null \n", __func__);
		    return 1;
		}
		//find primary chunk
		//chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
		//chunk = cb->remote_chunk.chunk_list[chunk_index];
		get_single(RDMABOX_sess, &ctx[0], NULL, replica_cb_list[0]->cb_index, cpuhint, 1, 0, 1);
		atomic_set(&ctx[0]->ref_count,0);

		sgt = &ctx[0]->data_tbl;
		sgt->sgl =  ctx[0]->sgl;
		sg = sgt->sgl;

		sg_init_table( ctx[0]->data_tbl.sgl, req->nr_phys_segments);
		retval = req_map_sg(req,  ctx[0]);
		if (retval) {
		    printk("failed to map sg\n");
		    put_cpu();
		    return 1;
		}
		nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sgt->sgl, sgt->nents, DMA_TO_DEVICE);
		if (!nents) {
		    sg_mark_end(sg);
		    sgt->nents = sgt->orig_nents;
		    put_cpu();
		    return -1;
		}

		for (i = 0; i < nents; i++) {
		    ctx[0]->rdma_sgl_wr[i].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
		    ctx[0]->rdma_sgl_wr[i].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
		    //ctx[0]->rdma_sgl_wr[i].lkey = cb->qp[0]->device->local_dma_lkey;
		    ctx[0]->rdma_sgl_wr[i].lkey = cb->qplist[cpuhint]->device->local_dma_lkey;
		    sg = sg_next(sg);
		}

		for(j=0; j<NUM_REPLICA; j++){
		    if(j>0){
			get_single(RDMABOX_sess, &ctx[j], NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
		    }
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
		    ctx[j]->rdma_sq_wr.wr.sg_list = &ctx[0]->rdma_sgl_wr[0];
		    ctx[j]->rdma_sq_wr.wr.num_sge = nents;
#else
		    ctx[j]->rdma_sq_wr.sg_list = &ctx[0]->rdma_sgl_wr[0];
		    ctx[j]->rdma_sq_wr.num_sge = nents;
#endif
		    atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
		    ctx[j]->prime_ctx = ctx[0];
		    ctx[j]->sending_item = NULL;
		    ctx[j]->replica_index = j;
		    ctx[j]->reqlist[0] = req;
		    ctx[j]->numreq = 1;
		    ctx[j]->isSGE = 1;
		    ctx[j]->len = nr_seg * RDMABOX_PAGE_SIZE;
		}//replica forloop

#ifdef DISKBACKUP
		stackbd_bio_generate_req(ctx[0], req);
#endif

		for(j=0; j<NUM_REPLICA; j++){
		    //ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint%NUM_CHANNEL);
		    ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint);
		    if (ret){
			put_cpu();
			return ret;
		    }
		}

		put_cpu();
		return 0;
	    }

int prep_preMR_write(struct RDMABOX_session *RDMABOX_sess, struct rdma_ctx **ctx, struct kernel_cb **replica_cb_list, int swapspace_index, int cpuhint, unsigned long start_idx_tmp_first, int debug)
{
        int retval = 0;
	int i,j,k;
	int ret;
        struct ib_send_wr *bad_wr;	
        int chunk_index;
        struct remote_chunk_g *chunk;
        unsigned long offset;
        size_t start;

	for(j=0; j<NUM_REPLICA; j++){
            start = blk_rq_pos(ctx[j]->reqlist[0]) << SECTOR_SHIFT;
            offset = start & ONE_GB_MASK;

	    if(j>0){
		get_single(RDMABOX_sess, &ctx[j], NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
		ctx[j]->mr = ctx[0]->mr;
		ctx[j]->len = ctx[0]->len;
	    }//j>0
            atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	    ctx[j]->sending_item = NULL;
	    ctx[j]->replica_index = j;
	    ctx[j]->isSGE = 0;
	    ctx[j]->prime_ctx = ctx[0];
	    ctx[j]->rdma_sgl_wr[0].addr = ctx[0]->mr->rdma_dma_addr;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx[j]->rdma_sq_wr.wr.num_sge = 1;
	    ctx[j]->rdma_sq_wr.wr.sg_list->length = ctx[0]->len;
#else
	    ctx[j]->rdma_sq_wr.num_sge = 1;
	    ctx[j]->rdma_sq_wr.sg_list->length = ctx[0]->len;
#endif
            chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[j][swapspace_index];
            chunk = replica_cb_list[j]->remote_chunk.chunk_list[chunk_index];

            ctx[j]->offset = offset;
            ctx[j]->cb = replica_cb_list[j];
            ctx[j]->chunk_ptr = chunk;
            ctx[j]->chunk_index = chunk_index;
            ctx[j]->rdma_sgl_wr[0].lkey = replica_cb_list[j]->qplist[cpuhint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
            ctx[j]->rdma_sq_wr.rkey = chunk->remote_rkey;
            ctx[j]->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
#else
            ctx[j]->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
            ctx[j]->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
#endif
	}//replica forloop

#ifdef DISKBACKUP
	struct bio *cloned_bio;
	cloned_bio = create_bio_copy(ctx[0]->reqlist[0]->bio);
	stackbd_bio_generate(ctx[0], cloned_bio, ctx[0]->len, start_idx_tmp_first );
	//stackbd_bio_generate_batch(ctx[0], start_idx_tmp_first);
#endif
/*
	for(j=0; j<NUM_REPLICA; j++){
            //printk("rdmabox[%s] post write swapspace_index:%d write_index:%d replica_index:%d cb_index:%d \n", __func__,swapspace_index,ctx->sending_item->start_index, replica_index,cb->cb_index);
            ret = ib_post_send(replica_cb_list[j]->qplist[cpuhint], &ctx[j]->rdma_sq_wr, &bad_wr);
            if (ret) {
                printk("rdmabox[%s]:client post write error rst=%d\n",__func__, ret);
                return ret;
             } 
	}
*/
	return 0;
}

int request_write(struct RDMABOX_session *RDMABOX_sess, struct request *req, int cpuhint)
{
    int retval = 0;
    int swapspace_index;
    int boundary_index;
    unsigned long offset;        
    int i,j,k;
    int ret;
    struct rdma_ctx *ctx[NUM_REPLICA];
    struct rdma_mr *mr;
    //struct timeval cur;
    struct kernel_cb *cb;
    //struct remote_chunk_g *chunk;
    //int chunk_index;
    unsigned char *cmem;
    char *buffer = NULL;
    struct kernel_cb *replica_cb_list[NUM_REPLICA];
    struct bio *tmpbio = req->bio;
    unsigned int nr_seg = req->nr_phys_segments;
    atomic_t *ref_count;

    size_t start = blk_rq_pos(req) << SECTOR_SHIFT;

    //unsigned long start_idx = blk_rq_pos(req) >> SECTORS_PER_PAGE_SHIFT;
    //printk("rdmabox[%s][%d]: start_idx=%d\n", __func__,cpuhint,start_idx);

#ifdef FLOW_CONTROL
retry_post:
    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	goto retry_post;
    }else{
	atomic_inc(&num_outstanding);
#endif

	swapspace_index = start >> ONE_GB_SHIFT;
	boundary_index = (start + (nr_seg*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;
	offset = start & ONE_GB_MASK;        

	// check two chunk
	if (swapspace_index != boundary_index) {
	    printk("rdmabox[%s]%s: two chunk. \n", __func__,__LINE__);
	    radix_write(RDMABOX_sess,req);
	    return 0;
	}//two chunk

	get_cpu();
	//find primary cb
	cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
	//printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
	if(!cb){
	    printk("rdmabox[%s]: cb is null \n", __func__);
	    panic("request_write : cb is null");
	    //return 1;
	}
	//find primary chunk
	//chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	//chunk = cb->remote_chunk.chunk_list[chunk_index];
	get_both(RDMABOX_sess, &ctx[0], &mr, replica_cb_list[0]->cb_index, cpuhint, 1, 1, 1);

	for(j=0; j<NUM_REPLICA; j++){
	    if(j==0){
		ctx[j]->mr = mr;
		atomic_set(&ctx[j]->ref_count,0);
		for (i=0; i < nr_seg;){
		    buffer = bio_data(tmpbio);
		    if(!buffer) {
			printk("buffer null. something wrong\n");
			put_cpu();
			//return 1;
			panic("request_write: bio_data null");
		    }
		    k = tmpbio->bi_phys_segments;
		    memcpy(ctx[j]->mr->rdma_buf + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * k);
		    i += k;
		    tmpbio = tmpbio->bi_next;
		}
	    }else{//j>0
		get_single(RDMABOX_sess, &ctx[j], NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
		ctx[j]->mr = ctx[0]->mr;
	    }//else

	    atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	    //ctx[j]->msg = msg;
	    ctx[j]->sending_item = NULL;
	    ctx[j]->replica_index = j;
	    ctx[j]->reqlist[0] = req;
	    ctx[j]->numreq = 1;
	    //ctx[j]->RDMABOX_sess = RDMABOX_sess;
	    ctx[j]->isSGE = 0;
	    ctx[j]->prime_ctx = ctx[0];
	    ctx[j]->len = nr_seg * RDMABOX_PAGE_SIZE;
	    ctx[j]->rdma_sgl_wr[0].addr = ctx[0]->mr->rdma_dma_addr;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx[j]->rdma_sq_wr.wr.sg_list->length = ctx[j]->len;
	    ctx[j]->rdma_sq_wr.wr.num_sge = 1;
#else
	    ctx[j]->rdma_sq_wr.sg_list->length = ctx[j]->len;
	    ctx[j]->rdma_sq_wr.num_sge = 1;
#endif
	}//replica forloop

#ifdef DISKBACKUP
	stackbd_bio_generate_mr(ctx[0], req);
#endif

	for(j=0; j<NUM_REPLICA; j++){
	    //ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint%NUM_CHANNEL);
	    ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint);
	    if (ret){
		put_cpu();
		return ret;
	    }
	}

	put_cpu();
	return 0;
}

int doorbell_remote_write(struct RDMABOX_session *RDMABOX_sess, struct rdma_ctx **ctx, struct kernel_cb **replica_cb_list, int swapspace_index, int cpuhint, unsigned long start_idx_tmp_first, int chunk_index)
{
	int retval = 0;
	int i,j=0,k=0;
	int ret;
	struct request *req;
	unsigned int nr_seg;
	unsigned long offset;

	struct ib_send_wr *bad_wr;	
	//int chunk_index;
	struct remote_chunk_g *chunk;
	size_t start;

	for(i=0; i<ctx[0]->numreq; i++){
	    //printk("rdmabox[%s]: ctx[%d] process. total:%d\n", __func__,i,ctx[0]->numreq);
	    req = ctx[i]->reqlist[0];

	    start = blk_rq_pos(req) << SECTOR_SHIFT;
	    offset = start & ONE_GB_MASK;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx[i]->rdma_sq_wr.wr.sg_list = &ctx[i]->rdma_sgl_wr[0];
	    if(i == ctx[0]->numreq - 1){
		//printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is null\n", __func__,i,ctx[1]->numreq);
		ctx[i]->rdma_sq_wr.wr.next = NULL; 
	    }else{
		//printk("rdmabox[%s]: ctx[%d] total:%d. connect rdma_sq_wr.next %d \n", __func__,i,ctx[0]->numreq, i+1);
		ctx[i]->rdma_sq_wr.wr.next = &ctx[i+1]->rdma_sq_wr; 
	    }
#else
	    ctx[i]->rdma_sq_wr.sg_list = &ctx[i]->rdma_sgl_wr[0];
	    if(i == ctx[0]->numreq - 1){
		//printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is null\n", __func__,i,ctx[0]->numreq);
		ctx[i]->rdma_sq_wr.next = NULL; 
	    }else{
		//printk("rdmabox[%s]: ctx[%d] total:%d. connect rdma_sq_wr.next %d \n", __func__,i,ctx[0]->numreq, i+1);
		ctx[i]->rdma_sq_wr.next = &ctx[i+1]->rdma_sq_wr; 
	    }
#endif

	    atomic_set(&ctx[i]->in_flight, CTX_W_IN_FLIGHT);
	    ctx[i]->prime_ctx = ctx[i];
	    ctx[i]->sending_item = NULL;
	    ctx[i]->replica_index = 0;

	    if(i>0)
		ctx[i]->numreq = 1;

	    ctx[i]->isSGE = 0;

	    //chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	    chunk = replica_cb_list[0]->remote_chunk.chunk_list[chunk_index];

	    ctx[i]->offset = offset;
	    ctx[i]->cb = replica_cb_list[0];
	    ctx[i]->chunk_ptr = chunk;
	    ctx[i]->chunk_index = chunk_index;
	    ctx[i]->rdma_sgl_wr[0].lkey = replica_cb_list[0]->qplist[cpuhint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx[i]->rdma_sq_wr.rkey = chunk->remote_rkey;
	    ctx[i]->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
	    ctx[i]->rdma_sq_wr.wr.num_sge = 1;
	    ctx[i]->rdma_sq_wr.wr.sg_list->length = ctx[i]->len;
#else
	    ctx[i]->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
	    ctx[i]->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
	    ctx[i]->rdma_sq_wr.num_sge = 1;
	    ctx[i]->rdma_sq_wr.sg_list->length = ctx[i]->len;
#endif
	    ctx[i]->rdma_sgl_wr[0].addr = ctx[i]->mr->rdma_dma_addr;
	}
	ctx[0]->numreq = 1;

	//printk("rdmabox[%s] post write swapspace_index:%d write_index:%d replica_index:%d cb_index:%d \n", __func__,swapspace_index,ctx->sending_item->start_index, replica_index,cb->cb_index);
	ret = ib_post_send(replica_cb_list[0]->qplist[cpuhint], &ctx[0]->rdma_sq_wr, &bad_wr);
	if (ret) {
	    printk("rdmabox[%s]: post write error. ret=%d\n",__func__, ret);
	    return ret;
	}

	return 0;
}

int doorbell_remote_write_sge(struct RDMABOX_session *RDMABOX_sess, struct rdma_ctx **ctx, struct kernel_cb **replica_cb_list, int swapspace_index, int qphint, unsigned long start_idx_tmp_first, int chunk_index)
{
	int retval = 0;
	int i,j=0,k=0;
	int ret;
	struct request *req;
	struct scatterlist *sg;
	struct sg_table *sgt;
	int nents;
	unsigned int nr_seg;
	unsigned long offset;

	struct ib_send_wr *bad_wr;	
	struct remote_chunk_g *chunk;
	size_t start;

	for(i=0; i<ctx[0]->numreq; i++){
	    //printk("rdmabox[%s]: ctx[%d] process sge total:%d\n", __func__,i,ctx[0]->numreq);
	    req = ctx[i]->reqlist[0];

	    start = blk_rq_pos(req) << SECTOR_SHIFT;
	    offset = start & ONE_GB_MASK;
	    nr_seg = req->nr_phys_segments;

	    sgt = &ctx[i]->data_tbl;
	    sgt->sgl =  ctx[i]->sgl;
	    sg = sgt->sgl;

	    sg_init_table( ctx[i]->data_tbl.sgl, req->nr_phys_segments);
	    retval = req_map_sg(req,  ctx[i]);
	    if (retval) {
		printk("failed to map sg\n");
		return 1;
	    }
	    nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sgt->sgl, sgt->nents, DMA_TO_DEVICE);
	    if (!nents) {
		sg_mark_end(sg);
		sgt->nents = sgt->orig_nents;
		printk("rdmabox[%s]: ctx[%d] fail to dma_map_sg\n", __func__,i);
		return -1;
	    }

	    for (k = 0; k < nents; k++) {
		ctx[i]->rdma_sgl_wr[k].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
		ctx[i]->rdma_sgl_wr[k].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
		ctx[i]->rdma_sgl_wr[k].lkey = replica_cb_list[0]->qp[0]->device->local_dma_lkey;
		sg = sg_next(sg);
	    }

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx[i]->rdma_sq_wr.wr.sg_list = &ctx[i]->rdma_sgl_wr[0];
	    ctx[i]->rdma_sq_wr.wr.num_sge = nents;
	    if(i == ctx[0]->numreq - 1){
		//printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is null\n", __func__,i,ctx[0]->numreq);
		ctx[i]->rdma_sq_wr.wr.next = NULL; 
	    }else{
		ctx[i]->rdma_sq_wr.wr.next = &ctx[i+1]->rdma_sq_wr; 
	    }
#else
	    ctx[i]->rdma_sq_wr.sg_list = &ctx[i]->rdma_sgl_wr[0];
	    ctx[i]->rdma_sq_wr.num_sge = nents;
	    if(i == ctx[0]->numreq - 1){
		//printk("rdmabox[%s]: ctx[%d] total:%d. rdma_sq_wr.next is null\n", __func__,i,ctx[0]->numreq);
		ctx[i]->rdma_sq_wr.next = NULL; 
	    }else{
		ctx[i]->rdma_sq_wr.next = &ctx[i+1]->rdma_sq_wr; 
	    }
#endif
	    atomic_set(&ctx[i]->in_flight, CTX_W_IN_FLIGHT);
	    ctx[i]->prime_ctx = ctx[i];
	    ctx[i]->sending_item = NULL;
	    ctx[i]->replica_index = 0;
	    ctx[i]->reqlist[0] = req;

	    if(i>0)
		ctx[i]->numreq = 1;

	    ctx[i]->isSGE = 1;
	    ctx[i]->len = nr_seg * RDMABOX_PAGE_SIZE;

	    //chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	    chunk = replica_cb_list[0]->remote_chunk.chunk_list[chunk_index];

	    ctx[i]->offset = offset;
	    ctx[i]->cb = replica_cb_list[0];
	    ctx[i]->chunk_ptr = chunk;
	    ctx[i]->chunk_index = chunk_index;
	    ctx[i]->rdma_sgl_wr[0].lkey = replica_cb_list[0]->qplist[qphint]->device->local_dma_lkey;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx[i]->rdma_sq_wr.rkey = chunk->remote_rkey;
	    ctx[i]->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
#else
	    ctx[i]->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
	    ctx[i]->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
#endif
	}
	ctx[0]->numreq = 1;

	//printk("rdmabox[%s] on qp%d post write swapspace_index:%d cb_index:%d \n", __func__,qphint%NUM_CHANNEL,swapspace_index,replica_cb_list[0]->cb_index);
	ret = ib_post_send(replica_cb_list[0]->qplist[qphint], &ctx[0]->rdma_sq_wr, &bad_wr);
	if (ret) {
	    printk("rdmabox[%s]: post write error. ret=%d\n",__func__, ret);
	    return ret;
	}

	return 0;
}

int remote_write(struct RDMABOX_session *RDMABOX_sess, struct rdma_ctx **ctx, struct kernel_cb **replica_cb_list, int swapspace_index, unsigned long offset, int cpuhint, unsigned long start_idx_tmp_first, int debug)
{
        int retval = 0;
	int i,j,k;
	int ret;
	struct ib_send_wr * bad_wr;
#ifdef SUPPORT_SGE_WRITE_BATCH
	struct request *req;
        struct scatterlist *sg;
        struct sg_table *sgt;
        int nents;
	int sg_pos;
        int chunk_index;
        struct remote_chunk_g *chunk;
#endif
	get_cpu();

#ifdef SUPPORT_SGE_WRITE_BATCH
    sgt = &ctx[0]->data_tbl;
    sgt->sgl = ctx[0]->sgl;
    sg = sgt->sgl;

    sg_init_table( ctx[0]->data_tbl.sgl, ctx[0]->total_segments);
    ctx[0]->data_tbl.nents = 0;
    sg_pos=0;

    for (i = 0; i < ctx[0]->numreq; i++) {
        req = ctx[0]->reqlist[i];
        ret = req_map_sg_each(req, ctx[0], sg);
        if (ret) {
            printk("failed to map sg\n");
            return 1;
        }
        nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sg, sgt->nents, DMA_TO_DEVICE);
        if (nents == 0) {
            printk("rdmabox[%s] : !nents req->segs:%d mapped sgt->nents:%d nents:%d\n",__func__,req->nr_phys_segments, sgt->nents, nents);
            sg_mark_end(sg);
            sgt->nents = sgt->orig_nents;
            return 1;
        }
	if(debug)
            printk("rdmabox[%s] : req:%p req->segs:%d totalseg:%d mapped sgt->nents:%d nents:%d\n",__func__,req, req->nr_phys_segments,ctx[0]->total_segments, sgt->nents, nents);

        for (k=sg_pos; k < (sg_pos+nents); k++) {
	    if(debug)
                printk("rdmabox[%s] : req:%p process sg[%d] nents:%d\n",__func__,req, k, nents);
            ctx[0]->rdma_sgl_wr[k].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
            ctx[0]->rdma_sgl_wr[k].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
            ctx[0]->rdma_sgl_wr[k].lkey = replica_cb_list[0]->qplist[cpuhint]->device->local_dma_lkey;
            sg_unmark_end(sg);
            sg = sg_next(sg);
        }
	sg_pos += nents;
    }//for
    sg_mark_end(sg);

    for(j=0; j<NUM_REPLICA; j++){
	   if(j>0){
		get_single(RDMABOX_sess, &ctx[j], NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
		ctx[j]->len = ctx[0]->len;
	   }//j>0
           atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	   ctx[j]->sending_item = NULL;
	   ctx[j]->replica_index = j;
	   ctx[j]->isSGE = 1;
	   ctx[j]->prime_ctx = ctx[0];

           chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[j][swapspace_index];
           chunk = replica_cb_list[j]->remote_chunk.chunk_list[chunk_index];
           ctx[j]->offset = offset;
           ctx[j]->cb = replica_cb_list[j];
           ctx[j]->chunk_ptr = chunk;
           ctx[j]->chunk_index = chunk_index;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
           ctx[j]->rdma_sq_wr.rkey = chunk->remote_rkey;
           ctx[j]->rdma_sq_wr.remote_addr = chunk->remote_addr + offset;
           //ctx[j]->rdma_sq_wr.wr.num_sge = sg_pos;
           ctx[j]->rdma_sq_wr.wr.num_sge = sgt->nents;
           ctx[j]->rdma_sq_wr.wr.sg_list = &ctx[0]->rdma_sgl_wr[0];
#else
           ctx[j]->rdma_sq_wr.wr.rdma.rkey = chunk->remote_rkey;
           ctx[j]->rdma_sq_wr.wr.rdma.remote_addr = chunk->remote_addr + offset;
           //ctx[j]->rdma_sq_wr.num_sge = sg_pos;
           ctx[j]->rdma_sq_wr.num_sge = sgt->nents;
           ctx[j]->rdma_sq_wr.sg_list = &ctx[0]->rdma_sgl_wr[0];
#endif
    }//replica forloop

#else //SUPPORT_SGE_WRITE_BATCH
	for(j=0; j<NUM_REPLICA; j++){
	    if(j>0){
		get_single(RDMABOX_sess, &ctx[j], NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
		ctx[j]->mr = ctx[0]->mr;
		ctx[j]->len = ctx[0]->len;
	   }//j>0
           atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	   ctx[j]->sending_item = NULL;
	    ctx[j]->replica_index = j;
	    ctx[j]->isSGE = 0;
	    ctx[j]->prime_ctx = ctx[0];
	    ctx[j]->rdma_sgl_wr[0].addr = ctx[0]->mr->rdma_dma_addr;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	    ctx[j]->rdma_sq_wr.wr.num_sge = 1;
	    ctx[j]->rdma_sq_wr.wr.sg_list->length = ctx[0]->len;
#else
	    ctx[j]->rdma_sq_wr.num_sge = 1;
	    ctx[j]->rdma_sq_wr.sg_list->length = ctx[0]->len;
#endif
	}//replica forloop

#endif //SUPPORT_SGE_WRITE_BATCH


#ifdef DISKBACKUP
	struct bio *cloned_bio;
	cloned_bio = create_bio_copy(ctx[0]->reqlist[0]->bio);
	stackbd_bio_generate(ctx[0], cloned_bio, ctx[0]->len, start_idx_tmp_first );
	//stackbd_bio_generate_batch(ctx[0], start_idx_tmp_first);
#endif

	for(j=0; j<NUM_REPLICA; j++){
#ifdef SUPPORT_SGE_WRITE_BATCH
            ret = ib_post_send(replica_cb_list[j]->qplist[cpuhint], &ctx[j]->rdma_sq_wr, &bad_wr);
#else
	    ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint);
#endif
            if (ret) {
                printk("rdmabox[%s]:client post write error rst=%d\n",__func__, ret);
                return ret;
             }
	}

	put_cpu();
	return 0;
}

int remote_sender_write(struct RDMABOX_session *RDMABOX_sess, struct local_page_list *tmp, int cpuhint)
{
	int retval = 0;
	int i;
	int swapspace_index, sec_swapspace_index;
	int boundary_index, sec_boundary_index;
	unsigned long chunk_offset, sec_chunk_offset;	
	struct local_page_list *sec_tmp;
	// start = byte offset , start_index = page offset
	unsigned long start = tmp->start_index << (SECTORS_PER_PAGE_SHIFT + SECTOR_SHIFT); // PAGE_SHIFT - SECTOR_SHIFT + SECTOR_SHIFT
	//size_t start = tmp->start_index << PAGE_SHIFT;

	swapspace_index = start >> ONE_GB_SHIFT;
	boundary_index = (start + (tmp->len*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;
	chunk_offset = start & ONE_GB_MASK;	

	// check two chunk
	if (swapspace_index != boundary_index) {
	    printk("rdmabox[%s]%s: two chunk. \n", __func__,__LINE__);
	    printk("rdmabox[%s]: index=%u, start=%u, len=%lu swapspace_index=%d, boundary_index=%d \n", __func__,tmp->start_index, start, tmp->len,swapspace_index,boundary_index);

repeat:
	    sec_tmp = get_free_item();
	    if(unlikely(!sec_tmp)){
		printk("[%s]: Fail to allocate list item. retry \n", __func__);
		goto repeat;
	    }

	    // find out the border
	    unsigned long border_index=0;
	    for(i=0; i<tmp->len ;i++){
		sec_boundary_index = (start + (border_index*RDMABOX_PAGE_SIZE)) >> ONE_GB_SHIFT;
		//size_t test_start = (tmp->start_index + border_index) << (SECTORS_PER_PAGE_SHIFT + SECTOR_SHIFT);
		//int test_swapspace_index = test_start >> ONE_GB_SHIFT;
		/*
		// TODO : debug code
		printk("rdmabox[%s]: i=%d, swapspace_index=%d, sec_boundary_index=%d test_swapspace_index=%d \n", __func__,i,swapspace_index,sec_boundary_index,test_swapspace_index);
		if(swapspace_index == sec_boundary_index){
		printk("rdmabox[%s]: i=%d, Same Chunk. \n", __func__,i);
		}else{
		//border_index = i;
		printk("rdmabox[%s]: i=%d, border_index=%u \n", __func__,i,border_index);
		break;
		}
		 */
		if(swapspace_index != sec_boundary_index)
		    break;

		border_index++;
	    }
	    // seperate tmp entry
	    int j=0;
	    for(i=border_index; i<tmp->len ;i++){
		//printk("rdmabox[%s]: i=%d len=%d \n",__func__,i,tmp->len);
		sec_tmp->batch_list[j] = tmp->batch_list[i];
		j++;
	    }
	    sec_tmp->start_index= tmp->start_index + border_index;
	    sec_tmp->len= tmp->len - border_index;
	    tmp->len= border_index;

	    printk("rdmabox[%s]: first_index=%u, first_len=%d second_index=%u second_len=%d \n", __func__,tmp->start_index,tmp->len,sec_tmp->start_index,sec_tmp->len);

	    unsigned long sec_start = sec_tmp->start_index << (SECTORS_PER_PAGE_SHIFT + SECTOR_SHIFT);
	    sec_swapspace_index = sec_start >> ONE_GB_SHIFT;
	    sec_chunk_offset = sec_start & ONE_GB_MASK;	

#ifdef DISKBACKUP
	    struct bio *cloned_bio;
	    cloned_bio = create_bio_copy(tmp->cloned_bio);
	    if (unlikely(!cloned_bio)) {
		printk("[%s]: fail to clone bio \n",__func__);
		return 1;
	    }
	    sec_tmp->cloned_bio = cloned_bio;
#endif

	    printk("rdmabox[%s]: sec_index=%u, sec_start=%u, sec_len=%lu sec_swapspace_index=%d \n", __func__,sec_tmp->start_index, sec_start, sec_tmp->len,sec_swapspace_index);

	    // send second half of entry
	    retval = rdma_write(RDMABOX_sess, sec_swapspace_index, sec_chunk_offset, sec_tmp, cpuhint); 
	    if (unlikely(retval)) {
		printk("rdmabox[%s]: failed to write on remote\n", __func__);
		return 1;
	    }
	}

	//printk("rdmabox[%s]: index=%u, start=%u, len=%lu swapspace_index=%d, boundary_index=%d \n", __func__,tmp->start_index, start, tmp->len,swapspace_index,boundary_index);

	retval = rdma_write(RDMABOX_sess, swapspace_index, chunk_offset, tmp, cpuhint); 
	if (unlikely(retval)) {
	    printk("rdmabox[%s]: failed to write on remote\n", __func__);
	    return 1;
	}
	return 0;
    }

    // mainly used to confirm that this device is not created; called before create device
struct RDMABOX_file *RDMABOX_file_find(struct RDMABOX_session *RDMABOX_session,
	    const char *xdev_name)
{
	struct RDMABOX_file *pos;
	struct RDMABOX_file *ret = NULL;

	spin_lock(&RDMABOX_session->devs_lock);
	list_for_each_entry(pos, &RDMABOX_session->devs_list, list) {
	    if (!strcmp(pos->file_name, xdev_name)) {
		ret = pos;
		break;
	    }
	}
	spin_unlock(&RDMABOX_session->devs_lock);

	return ret;
    }

// confirm that this portal (remote server port) is not used; called before create session
struct RDMABOX_session *RDMABOX_session_find_by_portal(struct list_head *s_data_list,
    const char *portal)
{
			struct RDMABOX_session *pos;
			struct RDMABOX_session *ret = NULL;

			mutex_lock(&g_lock);
			list_for_each_entry(pos, s_data_list, list) {
			    if (!strcmp(pos->portal, portal)) {
				ret = pos;
				break;
			    }
			}
			mutex_unlock(&g_lock);

			return ret;
}

//TODO : fix this
/*
static int RDMABOX_disconnect_handler(struct kernel_cb *cb)
{
		       int pool_index = cb->cb_index;
		       int i, j=0;
		       struct rdma_ctx *ctx_pool;
		       struct rdma_ctx *ctx;
		       struct RDMABOX_session *RDMABOX_sess = cb->RDMABOX_sess;
		       int *cb_chunk_map = cb->remote_chunk.chunk_map;
		       int sess_chunk_index;
		       int err = 0;
		       int evict_list[STACKBD_SIZE_G];
		       struct request *req;

		       for (i=0; i<STACKBD_SIZE_G;i++){
		       evict_list[i] = -1;
		       }

		    // for connected, but not mapped server
		    if (RDMABOX_sess->cb_state_list[cb->cb_index] == CB_CONNECTED){
		    printk("rdmabox[%s]: connected_cb [%d] is disconnected\n", __func__, cb->cb_index);

		    //need to clean cb info/struct
		    RDMABOX_sess->cb_state_list[cb->cb_index] = CB_FAIL;
		    return cb->cb_index;
		    }

		    //change cb state
		    RDMABOX_sess->cb_state_list[cb->cb_index] = CB_FAIL;
		    atomic_set(&cb->RDMABOX_sess->rdma_on, DEV_RDMA_OFF);

		    //disallow request to those cb chunks 
		    for (i = 0; i < MAX_MR_SIZE_GB; i++) {
		    sess_chunk_index = cb_chunk_map[i];
		    if (sess_chunk_index != -1) { //this cb chunk is mapped
		    evict_list[sess_chunk_index] = 1;
		    RDMABOX_bitmap_init(cb->remote_chunk.chunk_list[i]->bitmap_g); //should be in in_flight_thread
		    atomic_set(cb->remote_chunk.remote_mapped + i, CHUNK_UNMAPPED);
		    atomic_set(RDMABOX_sess->cb_index_map + (sess_chunk_index), NO_CB_MAPPED); 
		    printk("rdmabox[%s]: unmap chunk %d\n", __func__, sess_chunk_index);

		    }
		    }	

		    printk("rdmabox[%s]: unmap %d GB in cb%d \n", __func__, cb->remote_chunk.chunk_size_g, pool_index);

		    cb->remote_chunk.chunk_size_g = 0;

		    msleep(10);

		    for (i=0; i < submit_queues; i++){
		    ctx_pool = RDMABOX_sess->RDMABOX_conns[i]->ctx_pools[pool_index]->ctx_pool;
		    for (j=0; j < RDMABOX_QUEUE_DEPTH; j++){
		    ctx = ctx_pool + j;
		    switch (atomic_read(&ctx->in_flight)){
		    case CTX_R_IN_FLIGHT:
		    req = ctx->req;
		    atomic_set(&ctx->in_flight, CTX_IDLE);
		    printk("rdmabox[%s]: in_flight. call RDMABOX_mq_request_stackbd \n", __func__);

		    RDMABOX_mq_request_stackbd(req);
		    RDMABOX_insert_ctx(ctx);
		    break;
		    case CTX_W_IN_FLIGHT:
		    atomic_set(&ctx->in_flight, CTX_IDLE);
		    if (ctx->req == NULL){ 
		    break;
		    }
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
blk_mq_end_request(ctx->req, 0);
#else
		    blk_mq_end_io(ctx->req, 0);
#endif
		    break;
		default:
		    ;
	    }
	    }
		}	
		printk("rdmabox[%s]: finish handling in-flight request\n", __func__);


		for (i = 0; i < MAX_MR_SIZE_GB; i++) {
		    sess_chunk_index = cb_chunk_map[i];
		    if (sess_chunk_index != -1) { 
			RDMABOX_sess->mapping_swapspace_to_chunk[sess_chunk_index] = -1;
			RDMABOX_sess->free_chunk_index += 1;
			RDMABOX_sess->unmapped_chunk_list[RDMABOX_sess->free_chunk_index] = sess_chunk_index;
			cb_chunk_map[i] = -1;
		    }
		}

		//free conn->ctx_pools[cb_index]
		for (i =0; i<submit_queues; i++){
		    kfree(RDMABOX_sess->RDMABOX_conns[i]->ctx_pools[pool_index]->ctx_pool);
		    kfree(RDMABOX_sess->RDMABOX_conns[i]->ctx_pools[pool_index]->free_ctxs->ctx_list);
		    kfree(RDMABOX_sess->RDMABOX_conns[i]->ctx_pools[pool_index]->free_ctxs);
		    kfree(RDMABOX_sess->RDMABOX_conns[i]->ctx_pools[pool_index]);
		    RDMABOX_sess->RDMABOX_conns[i]->ctx_pools[pool_index] = (struct ctx_pool_list *)kzalloc(sizeof(struct ctx_pool_list), GFP_KERNEL);
		}

		atomic_set(&cb->RDMABOX_sess->rdma_on, DEV_RDMA_ON);
		for (i=0; i<STACKBD_SIZE_G; i++){
		    if (evict_list[i] == 1){
			RDMABOX_single_chunk_map(RDMABOX_sess, i);
		    }
		}


		printk("rdmabox[%s]: exit\n", __func__);

	return err;
}
*/

static int RDMABOX_cma_event_handler(struct rdma_cm_id *cma_id,
    struct rdma_cm_event *event)
{
			int ret;
			struct kernel_cb *cb = cma_id->context;

#ifdef DEBUG
			printk("rdmabox[%s]: cma_event type %d cma_id %p (%s)\n",__func__, event->event, cma_id,
				(cma_id == cb->cm_id) ? "parent" : "child");
#endif

			switch (event->event) {
			    case RDMA_CM_EVENT_ADDR_RESOLVED:
#ifdef DEBUG
				printk("rdmabox[%s]: RDMA_CM_EVENT_ADDR_RESOLVED \n",__func__);
#endif
				cb->state = ADDR_RESOLVED;
				ret = rdma_resolve_route(cma_id, 2000);
				if (ret) {
				    printk("rdmabox[%s]: rdma_resolve_route error %d\n",__func__,ret);
				    wake_up_interruptible(&cb->sem);
				}
				break;
			    case RDMA_CM_EVENT_ROUTE_RESOLVED:
#ifdef DEBUG
				printk("rdmabox[%s]: RDMA_CM_EVENT_ROUTE_RESOLVED  \n",__func__);
#endif
				cb->state = ROUTE_RESOLVED;
				wake_up_interruptible(&cb->sem);
				break;
			    case RDMA_CM_EVENT_CONNECT_REQUEST:
				cb->state = CONNECT_REQUEST;
				cb->child_cm_id = cma_id;
#ifdef DEBUG
				printk("rdmabox[%s]: RDMA_CM_EVENT_CONNECT_REQUEST child cma %p\n",__func__, cb->child_cm_id);
#endif
				wake_up_interruptible(&cb->sem);
				break;
			    case RDMA_CM_EVENT_ESTABLISHED:
#ifdef DEBUG
				printk("rdmabox[%s]: ESTABLISHED\n",__func__);
#endif
				cb->state = CONNECTED;
				wake_up_interruptible(&cb->sem);
				if (atomic_dec_and_test(&cb->RDMABOX_sess->conns_count)) {
#ifdef DEBUG
				    printk("rdmabox[%s]: ESTABLISHED last connection established\n", __func__);
#endif
				    complete(&cb->RDMABOX_sess->conns_wait);
				}
				break;

			    case RDMA_CM_EVENT_ADDR_ERROR:
			    case RDMA_CM_EVENT_ROUTE_ERROR:
			    case RDMA_CM_EVENT_CONNECT_ERROR:
			    case RDMA_CM_EVENT_UNREACHABLE:
			    case RDMA_CM_EVENT_REJECTED:
				printk("rdmabox[%s]: cma event %d, error %d\n",__func__, event->event,
					event->status);
				cb->state = ERROR;
				wake_up_interruptible(&cb->sem);
				break;
			    case RDMA_CM_EVENT_DISCONNECTED:	//should get error msg from here
				printk("rdmabox[%s]: DISCONNECT EVENT...\n",__func__);
				cb->state = CM_DISCONNECT;
				// RDMA is off
				//RDMABOX_disconnect_handler(cb);
				break;
			    case RDMA_CM_EVENT_DEVICE_REMOVAL:	//this also should be treated as disconnection, and continue disk swap
				printk("rdmabox[%s]: cma detected device removal!!!!\n",__func__);
				break;
			    default:
				printk("rdmabox[%s]: oof bad type!\n",__func__);
				wake_up_interruptible(&cb->sem);
				break;
			}
			return 0;
}

static int RDMABOX_chunk_wait_in_flight_requests(struct RDMABOX_session *RDMABOX_session, struct kernel_cb *cb, uint32_t rkey, int writeonlycheck)
{
		    int i,j,k;
		    int wr_active=0;
		    int rd_active=0;
		    struct rdma_ctx *ctx;
		    rdma_freepool_t *ctx_pool;

		    while (1) {
			// write pool
			for (j=0; j < submit_queues; j++){
			    //ctx_pool = &RDMABOX_session->rdma_write_freepool;
			    ctx_pool = cb->rdma_write_freepool_list[j];
			    for (i=0; i < ctx_pool->cap_nr_ctx; i++){
				ctx = (struct rdma_ctx *)ctx_pool->ctx_elements[i];
				if(!ctx){
				    //printk("rdmabox[%s]: ctx is null\n",__func__);
				    continue;
				}
				if(!ctx->chunk_ptr){
				    //printk("rdmabox[%s]: ctx->chunk_ptr is null\n",__func__);
				    continue;
				}
				if(ctx->chunk_ptr->remote_rkey != rkey){
				    //printk("rdmabox[%s]: ctx->chunk_ptr->remote_rkey is different\n",__func__);
				    continue;
				}
				if(atomic_read(&ctx->in_flight) == CTX_W_IN_FLIGHT){
				    printk("rdmabox[%s]: %d in write chunk_index %d\n", __func__, i, ctx->chunk_index);
				    wr_active += 1;
				}
			    }//for i
			}//for j

			if(!writeonlycheck){
			    // read pool
			    for (j=0; j < submit_queues; j++){
				//ctx_pool = &RDMABOX_session->rdma_read_freepool;
				ctx_pool = cb->rdma_read_freepool_list[j];
				for (i=0; i < ctx_pool->cap_nr_ctx; i++){
				    ctx = (struct rdma_ctx *)ctx_pool->ctx_elements[i];
				    if(!ctx){
					//printk("rdmabox[%s]: ctx is null\n",__func__);
					continue;
				    }
				    if(!ctx->chunk_ptr){
					//printk("rdmabox[%s]: ctx->chunk_ptr is null\n",__func__);
					continue;
				    }
				    if(ctx->chunk_ptr->remote_rkey != rkey){
					//printk("rdmabox[%s]: ctx->chunk_ptr->remote_rkey is different\n",__func__);
					continue;
				    }
				    if(atomic_read(&ctx->in_flight) == CTX_R_IN_FLIGHT){
					printk("rdmabox[%s]: %d in read chunk_index %d\n", __func__, i, ctx->chunk_index);
					rd_active += 1;
				    }
				}//for i
			    }//for j
			}// if read

			if (wr_active == 0 && rd_active == 0){
			    printk("rdmabox[%s]: all in_flight done wr:%d rd:%d \n", __func__,wr_active, rd_active);
			    return 0;
			}else{
			    printk("rdmabox[%s]: still waiting.. wr:%d rd:%d \n", __func__,wr_active, rd_active);
			    wr_active=0;
			    rd_active=0;
			}
		    }//while

		    return 1; 
}

int remote_sender_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
		    struct local_page_list *tmp_first=NULL;
		    int err;
		    int i;
		    int must_go = 0;

		    while(true){
			// take the item to send from sending list
			struct local_page_list *tmp;
			err = get_sending_index(&tmp);
			if(err){
			    if(tmp_first){
				//printk("rdmabox[%s]: No items more. Send tmp_first \n", __func__);
				tmp = NULL;
				goto send;
			    }
			    return 0;
			}

			if(!tmp_first)
			{
			    tmp_first=tmp;

			    if(must_go){
				tmp = NULL;
				goto send;
			    }else{
				continue;
			    }
			}

			// check if there is contiguous item
			// check if it fits into one item
			if( (tmp->start_index == tmp_first->start_index + tmp_first->len) &&
				(tmp_first->len + tmp->len <= RDMA_WR_BUF_LEN) )
			{
			    // merge items
			    int j = tmp_first->len;
			    for(i=0; i < tmp->len ;i++){
				tmp_first->batch_list[j] = tmp->batch_list[i];
				j++;
			    }
			    tmp_first->len = tmp_first->len + tmp->len;

			    // release unused item
			    put_free_item(tmp);
			    continue;
			}
send:
			// write remote
			err = remote_sender_write(RDMABOX_sess, tmp_first, cpuhint);
			if(err){
			    printk("rdmabox[%s]: remote write fail\n", __func__);
			    // TODO : fail exception handling. put sending list again ?
try_again:
			    err = put_sending_index(tmp_first);
			    if(err){
				printk("rdmabox[%s]: Fail to put sending index. retry \n", __func__);
				msleep(1);
				goto try_again;
			    }
			}

			if(!tmp){
			    must_go = 0;
			    return 0;
			}else{
			    must_go = 1;
			    tmp_first = tmp;
			}

		    }//while 
}

//int doorbell_batch_read_fn(void *arg)
int doorbell_batch_read_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
    //struct RDMABOX_session *RDMABOX_sess = arg;
    //int cpuhint = 0;
    int bitmap_i;
    struct request *tmp_first=NULL;
    int err, rst;
    int i,j;
    struct rdma_ctx *ctx[DOORBELL_BATCH_SIZE];
    int swapspace_index, swapspace_index_first;
    unsigned long start_idx_tmp, start_tmp;
    unsigned long start_idx_tmp_first, start_tmp_first;
    struct kernel_cb *cb, *cb_first;
    struct remote_chunk_g *chunk, *chunk_first;
    int chunk_index, chunk_index_first;
    struct kernel_cb *replica_cb_list[NUM_REPLICA];
    unsigned long offset, offset_first;
    int cb_index;
    int must_go = 0;
    struct rdma_mr *mr;

    //msleep(5000);
    //printk("rdmabox[%s]%s: thread starts \n", __func__,__LINE__);

#ifdef FLOW_CONTROL
retry_post:
    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	goto retry_post;
    }else{
	atomic_inc(&num_outstanding);
#endif

        get_cpu();
	while(true){
	    struct request *tmp;
	    err = get_read_req_item(&tmp);
	    if(err){
		if(tmp_first){	
		    must_go=1;
		    goto send;
		}
		//msleep(100);
		//continue;
                put_cpu();
		return 0;
	    }

	    start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT;
	    start_tmp = start_idx_tmp << (SECTORS_PER_PAGE_SHIFT + SECTOR_SHIFT);
	    swapspace_index = start_tmp >> ONE_GB_SHIFT;
	    //printk("[%s]: Incoming Req:%x start_idx_tmp=%lu\n", __func__,tmp,start_idx_tmp);

	    // for now, we don't consier replica. cb_index_map[0] is primary
	    cb_index = atomic_read(RDMABOX_sess->cb_index_map[0] + swapspace_index);
	    if(cb_index == NO_CB_MAPPED){
		//printk("rdmabox[%s]: NO_CB_MAPPED. do nothing. start_idx_tmp:%lu cb_index:%d, swapspace_index:%d\n", __func__,start_idx_tmp,cb_index,swapspace_index);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
		blk_mq_end_request(tmp, 0);
#else
		blk_mq_end_io(tmp, 0);
#endif
		continue;
	    }
	    cb = RDMABOX_sess->cb_list[cb_index];

	    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	    chunk = cb->remote_chunk.chunk_list[chunk_index];

	    offset = start_tmp & ONE_GB_MASK;	
	    bitmap_i = (int)(offset / RDMABOX_PAGE_SIZE);

	    // check if it is in remote
	    rst = RDMABOX_bitmap_test(chunk->bitmap_g, bitmap_i);
	    if(!rst){
		//printk("rdmabox[%s]%s: No remote mapping. Go check local \n", __func__,__LINE__);
		err = radix_read(RDMABOX_sess,tmp);
		if(err){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
		    blk_mq_end_request(tmp, 0);
#else
		    blk_mq_end_io(tmp, 0);
#endif
		}
		continue;
	    }

	    if(!tmp_first)
	    {
reload:
		tmp_first=tmp;
		cb_first = cb;
		offset_first = offset;
		swapspace_index_first = swapspace_index;
		chunk_index_first = chunk_index;
		chunk_first = chunk;

		start_idx_tmp_first = blk_rq_pos(tmp_first) >> SECTORS_PER_PAGE_SHIFT;

#if defined(USE_DOORBELL_READ)
		// get prime ctx and mr
		get_both(RDMABOX_sess, &ctx[0], &mr, cb_first->cb_index, cpuhint, 1, 1, 0);
		ctx[0]->mr = mr;
#elif defined(USE_DOORBELL_READ_SGE)
		get_single(RDMABOX_sess , &ctx[0], NULL, cb_first->cb_index, cpuhint,1, 0, 0);
#endif
		ctx[0]->reqlist[ctx[0]->numreq++] = tmp_first;
		ctx[0]->len = RDMABOX_PAGE_SIZE;

		if(must_go){
		    goto send;
		}else{
		    continue;
		}
	    }

	    if (swapspace_index_first != swapspace_index) {
		goto send;
	    }

	    // check if there is contiguous item
	    if(ctx[0]->numreq < DOORBELL_BATCH_SIZE)
	    {
#if defined(USE_DOORBELL_READ)
		// get prime ctx and mr
		get_both(RDMABOX_sess, &ctx[ctx[0]->numreq], &mr, cb_first->cb_index, cpuhint, 1, 1, 0);
		ctx[ctx[0]->numreq]->mr = mr;
#elif defined(USE_DOORBELL_READ_SGE)
		get_single(RDMABOX_sess , &ctx[ctx[0]->numreq], NULL, cb_first->cb_index, cpuhint,1, 0, 0);
#endif
		ctx[ctx[0]->numreq]->len = RDMABOX_PAGE_SIZE;
		ctx[ctx[0]->numreq++]->reqlist[0] = tmp;

		//printk("rdmabox[%s]: merge start_idx_tmp=%lu numreq:%d start_tmp=%lu \n",__func__,start_idx_tmp,ctx[0]->numreq, start_tmp);
		continue;
	    }

send:
#if defined(USE_DOORBELL_READ)
	    err = doorbell_remote_read(RDMABOX_sess, ctx, cb_first, chunk_index_first, chunk_first, offset_first, swapspace_index_first, cpuhint);
#elif defined(USE_DOORBELL_READ_SGE)
	    err = doorbell_remote_read_sge(RDMABOX_sess, ctx, cb_first, chunk_index_first, chunk_first, offset_first, swapspace_index_first, cpuhint);
#endif
	    if(err){
		printk("rdmabox[%s]: remote read fail\n", __func__);
		panic("remote read fail");
	    }
	    if(must_go){
		//must_go = 0;
		//tmp_first=NULL;
		//continue;
                put_cpu();
		return 0;
	    }else{
		must_go = 1;
		goto reload;
	    }

	}//while 

        put_cpu();
	return 0;
}

int batch_read_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
	int bitmap_i;
	struct request *tmp_first=NULL;
	int err, rst;
	int i,j;
	struct rdma_ctx *ctx;
#ifndef SUPPORT_SGE_READ_BATCH
	struct rdma_mr *mr;
#endif
	int swapspace_index, swapspace_index_first;
	unsigned long start_idx_tmp, start_tmp;
	unsigned long start_idx_tmp_first, start_tmp_first;
	struct kernel_cb *cb, *cb_first;
	struct remote_chunk_g *chunk, *chunk_first;
	int chunk_index, chunk_index_first;
	struct kernel_cb *replica_cb_list[NUM_REPLICA];
	unsigned long offset, offset_first;
	int cb_index;
	int must_go = 0;

#ifdef FLOW_CONTROL
retry_post:
	if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	    goto retry_post;
	}else{
	    atomic_inc(&num_outstanding);
#endif

	    get_cpu();
	    while(true){
		struct request *tmp;
		err = get_read_req_item(&tmp);
		if(err){
		    if(tmp_first){	
			tmp = NULL;
			goto send;
		    }
		    put_cpu();
		    return 0;
		}

		start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT;
		start_tmp = start_idx_tmp << (SECTORS_PER_PAGE_SHIFT + SECTOR_SHIFT);
		swapspace_index = start_tmp >> ONE_GB_SHIFT;
		//printk("[%s]: Incoming Req:%x start_idx_tmp=%lu\n", __func__,tmp,start_idx_tmp);

		// for now, we don't consier replica. cb_index_map[0] is primary
		cb_index = atomic_read(RDMABOX_sess->cb_index_map[0] + swapspace_index);
		if(cb_index == NO_CB_MAPPED){
		    //printk("rdmabox[%s]: NO_CB_MAPPED. do nothing. start_idx_tmp:%lu cb_index:%d, swapspace_index:%d\n", __func__,start_idx_tmp,cb_index,swapspace_index);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
		    blk_mq_end_request(tmp, 0);
#else
		    blk_mq_end_io(tmp, 0);
#endif
		    put_cpu();
		    return 0;
		}
		cb = RDMABOX_sess->cb_list[cb_index];

		chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
		chunk = cb->remote_chunk.chunk_list[chunk_index];
/*
		   if(chunk->chunk_state == NONE_ALLOWED){
		   printk("rdmabox[%s]: this chunk is READONLY. MIGRATION on progress\n",__func__);
try_again:
		err = put_read_req_item(tmp);
		if(err){
			printk("rdmabox[%s]: Fail to put req item. retry \n", __func__);
			goto try_again;
		}
		continue;
		}
*/
		offset = start_tmp & ONE_GB_MASK;	
		bitmap_i = (int)(offset / RDMABOX_PAGE_SIZE);

		// check if it is in remote
		rst = RDMABOX_bitmap_test(chunk->bitmap_g, bitmap_i);
		if(!rst){
		    //printk("rdmabox[%s]%s: No remote mapping. Go check local \n", __func__,__LINE__);
		    err = radix_read(RDMABOX_sess,tmp);
		    if(err){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
			blk_mq_end_request(tmp, 0);
#else
			blk_mq_end_io(tmp, 0);
#endif
		    }
		    put_cpu();
		    return 0;
		}

	if(!tmp_first)
	{
	reload:
    tmp_first=tmp;
    cb_first = cb;
    offset_first = offset;
    swapspace_index_first = swapspace_index;
    chunk_index_first = chunk_index;
    chunk_first = chunk;

    start_idx_tmp_first = blk_rq_pos(tmp_first) >> SECTORS_PER_PAGE_SHIFT;

#if defined(SUPPORT_SGE_READ_BATCH)
    get_single(RDMABOX_sess , &ctx, NULL, cb_first->cb_index, cpuhint,1, 0, 0);
#else
    get_both(RDMABOX_sess, &ctx, &mr, cb_first->cb_index, cpuhint, 1, 1, 0);
    ctx->mr = mr;
#endif
    ctx->reqlist[ctx->numreq++] = tmp_first;
    ctx->len = RDMABOX_PAGE_SIZE;

    if(must_go){
	tmp = NULL;
	goto send;
    }else{
	continue;
    }
}

if (swapspace_index_first != swapspace_index) {
    goto send;
}

// check if there is contiguous item
if( (start_idx_tmp == (start_idx_tmp_first + ctx->numreq) ) &&
	((ctx->numreq + 1) <= PAGE_CLUSTER) )
{
    ctx->reqlist[ctx->numreq++] = tmp;
    ctx->len = ctx->len + RDMABOX_PAGE_SIZE;
    //printk("rdmabox[%s]: merge start_idx_tmp=%lu numreq:%d start_tmp=%lu \n",__func__,start_idx_tmp,numreq, start_tmp);
    continue;
}

send:
//err = remote_read(RDMABOX_sess, ctx, cb_first, chunk_index_first, chunk_first, offset_first, swapspace_index_first, cpuhint%NUM_CHANNEL);
err = remote_read(RDMABOX_sess, ctx, cb_first, chunk_index_first, chunk_first, offset_first, swapspace_index_first, cpuhint);
if(err){
    printk("rdmabox[%s]: remote read fail\n", __func__);
    panic("remote read fail");
}
if(!tmp){
    must_go = 0;
    put_cpu();
    return 0;
}else{
    must_go = 1;
    goto reload;
}

}//while 

return 0;
}

int hybrid_batch_read_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
    int bitmap_i;
    struct request *tmp_first=NULL;
    int err, rst;
    int i,j;
    struct rdma_ctx *ctx;
    struct rdma_mr *mr;
    int swapspace_index, swapspace_index_first;
    unsigned long start_idx_tmp, start_tmp;
    unsigned long start_idx_tmp_first, start_tmp_first;
    struct kernel_cb *cb, *cb_first;
    struct remote_chunk_g *chunk, *chunk_first;
    int chunk_index, chunk_index_first;
    struct kernel_cb *replica_cb_list[NUM_REPLICA];
    unsigned long offset, offset_first;
    int cb_index;
    int must_go = 0;
    int isSGE;

#ifdef FLOW_CONTROL
retry_post:
    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	goto retry_post;
    }else{
	atomic_inc(&num_outstanding);
#endif

	get_cpu();
	while(true){
	    struct request *tmp;
	    err = get_read_req_item(&tmp);
	    if(err){
		if(tmp_first){	
		    tmp = NULL;
		    goto send;
		}
		put_cpu();
		return 0;
	    }

	    start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT;
	    start_tmp = start_idx_tmp << (SECTORS_PER_PAGE_SHIFT + SECTOR_SHIFT);
	    swapspace_index = start_tmp >> ONE_GB_SHIFT;
	    //printk("[%s]: Incoming Req:%x start_idx_tmp=%lu\n", __func__,tmp,start_idx_tmp);

	    // for now, we don't consier replica. cb_index_map[0] is primary
	    cb_index = atomic_read(RDMABOX_sess->cb_index_map[0] + swapspace_index);
	    if(cb_index == NO_CB_MAPPED){
		//printk("rdmabox[%s]: NO_CB_MAPPED. do nothing. start_idx_tmp:%lu cb_index:%d, swapspace_index:%d\n", __func__,start_idx_tmp,cb_index,swapspace_index);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
		blk_mq_end_request(tmp, 0);
#else
		blk_mq_end_io(tmp, 0);
#endif
		put_cpu();
		return 0;
	    }
	    cb = RDMABOX_sess->cb_list[cb_index];

	    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	    chunk = cb->remote_chunk.chunk_list[chunk_index];
	    if(chunk->chunk_state == NONE_ALLOWED){
		printk("rdmabox[%s]: this chunk is READONLY. MIGRATION on progress\n",__func__);
try_again:
		err = put_read_req_item(tmp);
		if(err){
		    printk("rdmabox[%s]: Fail to put req item. retry \n", __func__);
		    goto try_again;
		}
		continue;
	    }

	    offset = start_tmp & ONE_GB_MASK;	
	    bitmap_i = (int)(offset / RDMABOX_PAGE_SIZE);

	    // check if it is in remote
	    rst = RDMABOX_bitmap_test(chunk->bitmap_g, bitmap_i);
	    if(!rst){
		//printk("rdmabox[%s]%s: No remote mapping. Go check local \n", __func__,__LINE__);
		err = radix_read(RDMABOX_sess,tmp);
		if(err){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
		    blk_mq_end_request(tmp, 0);
#else
		    blk_mq_end_io(tmp, 0);
#endif
		}
		put_cpu();
		return 0;
	    }

	    if(!tmp_first)
	    {
reload:
		tmp_first=tmp;
		cb_first = cb;
		offset_first = offset;
		swapspace_index_first = swapspace_index;
		chunk_index_first = chunk_index;
		chunk_first = chunk;

		start_idx_tmp_first = blk_rq_pos(tmp_first) >> SECTORS_PER_PAGE_SHIFT;

		isSGE = get_both(RDMABOX_sess, &ctx, &mr, cb_first->cb_index, cpuhint, 1, 1, 0);
		ctx->isSGE = isSGE;

		if(isSGE)
		    printk("rdmabox[%s] failed to get MR. Do SGE.\n",__func__);

		ctx->mr = mr;
		ctx->reqlist[ctx->numreq++] = tmp_first;
		ctx->len = RDMABOX_PAGE_SIZE;

		if(must_go){
		    tmp = NULL;
		    goto send;
		}else{
		    continue;
		}
	    }

	    if (swapspace_index_first != swapspace_index) {
		goto send;
	    }

	    // check if there is contiguous item
	    if( (start_idx_tmp == (start_idx_tmp_first + ctx->numreq) ) &&
		    ((ctx->numreq + 1) <= PAGE_CLUSTER) )
	    {
		ctx->reqlist[ctx->numreq++] = tmp;
		ctx->len = ctx->len + RDMABOX_PAGE_SIZE;
		//printk("rdmabox[%s]: merge start_idx_tmp=%lu numreq:%d start_tmp=%lu \n",__func__,start_idx_tmp,numreq, start_tmp);
		continue;
	    }

send:
	    //err = hybrid_remote_read(RDMABOX_sess, ctx, cb_first, chunk_index_first, chunk_first, offset_first, swapspace_index_first, cpuhint%NUM_CHANNEL);
	    err = hybrid_remote_read(RDMABOX_sess, ctx, cb_first, chunk_index_first, chunk_first, offset_first, swapspace_index_first, cpuhint);
	    if(err){
		printk("rdmabox[%s]: remote read fail\n", __func__);
		panic("remote read fail");
	    }
	    if(!tmp){
		must_go = 0;
		put_cpu();
		return 0;
	    }else{
		must_go = 1;
		goto reload;
	    }

	}//while 

	return 0;
    }

/*
int pre_check_read(struct RDMABOX_session *RDMABOX_sess, struct request *tmp, int cpuhint)
{
       int cb_index;
       int swapspace_index;
       int boundary_index;
       unsigned long start_idx_tmp, start_tmp;

	start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT;
	start_tmp = start_idx_tmp << (SECTORS_PER_PAGE_SHIFT + SECTOR_SHIFT);
	swapspace_index = start_tmp >> ONE_GB_SHIFT;
	//printk("[%s]: Incoming Req:%x start_idx_tmp=%lu\n", __func__,tmp,start_idx_tmp);

	// for now, we don't consier replica. cb_index_map[0] is primary
	cb_index = atomic_read(RDMABOX_sess->cb_index_map[0] + swapspace_index);
	if(cb_index == NO_CB_MAPPED){
	    //printk("rdmabox[%s]: NO_CB_MAPPED. do nothing. start_idx_tmp:%lu cb_index:%d, swapspace_index:%d\n", __func__,start_idx_tmp,cb_index,swapspace_index);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
	    blk_mq_end_request(tmp, 0);
#else
	    blk_mq_end_io(tmp, 0);
#endif
	    return 1;
	}
	return 0;
}
*/

int pre_check_write(struct RDMABOX_session *RDMABOX_sess, struct request *tmp, int cpuhint)
{
       int swapspace_index;
       int boundary_index;
       unsigned long start_idx_tmp, start_tmp;
/*
#ifdef SUPPORT_SGE_WRITE_BATCH
if(tmp->nr_phys_segments == MAX_SECTORS){
//printk("rdmabox[%s]%s: SGE. start_idx:%lu nr_phy:%d \n", __func__,__LINE__,start_idx_tmp,tmp->nr_phys_segments);
request_write(RDMABOX_sess,tmp,cpuhint);
return 1;
}
#endif
*/
    // check two chunk
    start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT; // 3
    start_tmp = start_idx_tmp << PAGE_SHIFT;
    swapspace_index = start_tmp >> ONE_GB_SHIFT;
    boundary_index = (start_tmp + (tmp->nr_phys_segments*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;

    if (swapspace_index != boundary_index) {
        printk("rdmabox[%s]%s: tmp has two chunk. \n", __func__,__LINE__);
        radix_write(RDMABOX_sess,tmp);
        return 1;
    }

    return 0;
}

/*
int prepare_write(struct request *tmp, int swapspace_index, struct rdma_ctx **ctx, struct kernel_cb **replica_cb_list, int cpuhint)
{
    int i;
    struct rdma_mr *mr;

    //find primary cb
    cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
    //printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
    if(!cb){
    printk("[%s]: cb is null \n", __func__);
    return 1;
    }

    // get prime ctx and mr
    get_both(RDMABOX_sess, ctx, &mr, replica_cb_list_first[0]->cb_index, cpuhint, 1);
    for(i=0; i<NUM_REPLICA; i++)
    {
    if(i==0){
    ctx[j]->mr = mr;
    atomic_set(&ctx[0]->ref_count,0);
    }
    }

    return 0;
    }
*/

/*
// save working code for appr3(hyb with preMR and dynMR + doorbell)
int new_hybrid_batch_write_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
    struct request *tmp_first=NULL;
    int err;
    int i,j;
    struct rdma_ctx *ctx[NUM_REPLICA];
    struct rdma_ctx **ctxlist[DOORBELL_BATCH_SIZE];
    int batch_cnt=0;
    struct rdma_mr *mr;
    struct bio *tmpbio;
    char *buffer;
    int cur_offset;
    int swapspace_index, swapspace_index_first;
    int boundary_index;
    unsigned long start_idx_tmp, start_tmp;
    unsigned long start_idx_tmp_first, start_tmp_first;
    struct kernel_cb *cb;
    struct remote_chunk_g *chunk;
    int chunk_index;
    struct kernel_cb *replica_cb_list[NUM_REPLICA];
    struct kernel_cb *replica_cb_list_first[NUM_REPLICA];
    unsigned char *cmem;
    int must_go = 0;
    int debug=0;
    int mode;
    struct ib_send_wr * bad_wr;

#ifdef FLOW_CONTROL
retry_post:
    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	goto retry_post;
    }else{
	atomic_inc(&num_outstanding);
#endif
	get_cpu();
	while(batch_cnt < DOORBELL_BATCH_SIZE){
	    struct request *tmp;
	    err = get_req_item(&tmp);
	    if(err){
		if(tmp_first){	
		    tmp = NULL;
		    goto send;
		}
		put_cpu();
		return 0;
	    }
	    // check two chunk
	    start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT; // 3
	    start_tmp = start_idx_tmp << PAGE_SHIFT;
	    swapspace_index = start_tmp >> ONE_GB_SHIFT;
	    boundary_index = (start_tmp + (tmp->nr_phys_segments*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;

	    if (swapspace_index != boundary_index) {
		printk("rdmabox[%s]%s: tmp has two chunk. \n", __func__,__LINE__);
		radix_write(RDMABOX_sess,tmp);
		continue;
	    }

	    //printk("[%s]: Incoming Req:%p start_idx_tmp=%lu nr_segs=%d \n", __func__,tmp,start_idx_tmp, tmp->nr_phys_segments);

	    //find primary cb
	    cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
	    //printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
	    if(!cb){
		printk("[%s]: cb is null \n", __func__);
		put_cpu();
		return 1;
	    }

	    //find primary chunk
	    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	    chunk = replica_cb_list[0]->remote_chunk.chunk_list[chunk_index];

	    // check if READONLY
	    if(chunk->chunk_state >= READONLY){
	    	printk("rdmabox[%s]: this chunk is READONLY. MIGRATION on progress\n",__func__);
try_again:
   	    	err = put_req_item(tmp);
	    	if(err){
		    printk("[%s]: Fail to put req item. retry \n", __func__);
		    goto try_again;
	        }
		continue;
	    }
 
	    if (tmp_first && swapspace_index_first != swapspace_index) {
		printk("rdmabox[%s]%s: tmp has different swapspace_index \n", __func__,__LINE__);
		goto send;
	    }

	    if(tmp->nr_phys_segments > MAX_SGE){
		if(mode == 0){
                    mode = 1;
		    printk("rdmabox[%s]%s: mode changes dynMR -> preMR segs:%d\n", __func__,__LINE__,tmp->nr_phys_segments);
    		    if(tmp_first){	
		        printk("rdmabox[%s]%s: change tmp_first with tmp:%p\n", __func__,__LINE__,tmp);
		        goto reload;
		    }
		}
	    }else{
    	        if(mode == 1){
		    mode = 0;
		    printk("rdmabox[%s]%s: mode changes preMR -> dynMR segs:%d\n", __func__,__LINE__,tmp->nr_phys_segments);
    		    if(tmp_first){	
		        printk("rdmabox[%s]%s: change tmp_first with tmp:%p\n", __func__,__LINE__,tmp);
                        mode = 0;
		        goto reload;
		    }
		}
	    }

	if(!tmp_first)
	{
reload:
	    tmp_first=tmp;
	    if(mode==1){
	        tmpbio = tmp_first->bio;
	    }
	    cur_offset = 0;
	    swapspace_index_first = swapspace_index;
	    for (i=0; i<NUM_REPLICA; i++){
		replica_cb_list_first[i] = replica_cb_list[i];
	    }
	    start_idx_tmp_first = start_idx_tmp;

	    if(mode==0){
	        get_single(RDMABOX_sess, &ctx[0], NULL, replica_cb_list_first[0]->cb_index, cpuhint, 1, 0, 1);
		ctx[0]->isSGE=1;
	    }
	    if(mode==1){
	        // get prime ctx and mr
	        get_both(RDMABOX_sess, &ctx[0], &mr, replica_cb_list_first[0]->cb_index, cpuhint, 1, 1, 1);
	        ctx[0]->mr = mr;
		ctx[0]->isSGE=0;

	        for (i=0; i < tmp_first->nr_phys_segments;){
		    buffer = bio_data(tmpbio);
		    j = tmpbio->bi_phys_segments;
		    memcpy(ctx[0]->mr->rdma_buf + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
		    i += j;
		    tmpbio = tmpbio->bi_next;
   	        }
	    }

	    ctx[0]->numreq = 0;
	    atomic_set(&ctx[0]->ref_count,0);
 	    cur_offset = tmp_first->nr_phys_segments;
	    if(mode==0){
	        ctx[0]->total_segments = cur_offset;
	    }
	    ctx[0]->reqlist[ctx[0]->numreq++] = tmp_first;
	    ctx[0]->len = tmp_first->nr_phys_segments * RDMABOX_PAGE_SIZE;
            ctxlist[batch_cnt++] = ctx;

	    if(must_go){
		tmp = NULL;
		goto send;
	    }else{
		continue;
   	    }
	}

	// check if it is next contiguous item
	// check if it fits into one item
        if(mode==0){ // dynMR

	    // FIXME : null crash on sg_next when merging. for now we use single IO on sge write
	    if( (start_idx_tmp == start_idx_tmp_first + cur_offset) &&
		(cur_offset + tmp->nr_phys_segments <= MAX_SGE) )
	    {
	        cur_offset = cur_offset + tmp->nr_phys_segments;
                ctx[0]->total_segments = cur_offset;
	        ctx[0]->reqlist[ctx[0]->numreq++] = tmp;
	        ctx[0]->len = ctx[0]->len + (tmp->nr_phys_segments * RDMABOX_PAGE_SIZE);
	        printk("rdmabox[%s]: merge. req:%p start_idx_tmp:%lu nr_segs:%d numreq:%d tmp_first_nr_seg:%d \n", __func__,tmp, start_idx_tmp,tmp->nr_phys_segments,ctx[0]->numreq,tmp_first->nr_phys_segments);
	        debug=1;
                continue;
	    }
	}
        if(mode==1){ // preMR
	    if( (start_idx_tmp == start_idx_tmp_first + cur_offset) &&
		(cur_offset + tmp->nr_phys_segments <= RDMA_WR_BUF_LEN)  )
	    {
       	        tmpbio = tmp->bio;
	        // merge items
	        for(i=0; i < tmp->nr_phys_segments ;){
		    buffer = bio_data(tmpbio);
		    j = tmpbio->bi_phys_segments;
		    memcpy(ctx[0]->mr->rdma_buf + ctx[0]->len + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
		    i += j;
		    tmpbio = tmpbio->bi_next;
    	        }
	        cur_offset = cur_offset + tmp->nr_phys_segments;
	        ctx[0]->reqlist[ctx[0]->numreq++] = tmp;
	        ctx[0]->len = ctx[0]->len + (tmp->nr_phys_segments * RDMABOX_PAGE_SIZE);
	        printk("rdmabox[%s]: merge. req:%p start_idx_tmp:%lu nr_segs:%d numreq:%d tmp_first_nr_seg:%d \n", __func__,tmp, start_idx_tmp,tmp->nr_phys_segments,ctx[0]->numreq,tmp_first->nr_phys_segments);
	        debug=0;
	        continue;
	    }
	}
	// if gets here, fail to preMR or dynMR batch, now we do doorbell batch
        // for now, no replica support
        if(mode==0){
            get_single(RDMABOX_sess, &ctx[0], NULL, replica_cb_list[0]->cb_index, cpuhint, 1, 0, 1);
	    ctx[0]->isSGE=1;
        }
        if(mode==1){
            // get prime ctx and mr
            get_both(RDMABOX_sess, &ctx[0], &mr, replica_cb_list[0]->cb_index, cpuhint, 1, 1, 1);
            ctx[0]->mr = mr;
	    ctx[0]->isSGE=0;

            for (i=0; i < tmp->nr_phys_segments;){
	        buffer = bio_data(tmpbio);
	        j = tmpbio->bi_phys_segments;
	        memcpy(ctx[0]->mr->rdma_buf + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
	        i += j;
	        tmpbio = tmpbio->bi_next;
            }
        }

        ctx[0]->numreq = 0;
        atomic_set(&ctx[0]->ref_count,0);
        if(mode==0){
            ctx[0]->total_segments = tmp->nr_phys_segments;
	}
	ctx[0]->reqlist[ctx[0]->numreq++] = tmp;
	ctx[0]->len = tmp->nr_phys_segments * RDMABOX_PAGE_SIZE;

        ctxlist[batch_cnt++] = ctx;
        printk("rdmabox[%s]:doorbell merge ctxlist[%d]:%p \n",__func__, batch_cnt, ctx[0]);
        continue;
	   
send:
        if(batch_cnt == 1){
            ctxlist[0][0]->rdma_sq_wr.next = NULL;
            if(mode == 0){
                //printk("rdmabox[%s]: single dynMR prep write. req:%p numreq:%d swapspace:%lu \n", __func__,ctxlist[0][0]->reqlist[0],ctxlist[0][0]->numreq,swapspace_index_first);
	        prep_dynMR_write(RDMABOX_sess, ctxlist[0], replica_cb_list_first, swapspace_index_first, cpuhint, start_idx_tmp_first, debug);
	    }else{
                //printk("rdmabox[%s]: single preMR prep write. req:%p numreq:%d swapspace:%lu \n", __func__,ctxlist[0][0]->reqlist[0],ctxlist[0][0]->numreq,swapspace_index_first);
                prep_preMR_write(RDMABOX_sess, ctxlist[0], replica_cb_list_first, swapspace_index_first, cpuhint, start_idx_tmp_first, 0);
	    }

	}else{

	    for(i=0; i<batch_cnt; i++){
            
                if(ctxlist[i][0]->isSGE){
                    printk("rdmabox[%s]: batch_cnt:%d/%d dynMR prep write. req:%p numreq:%d swapspace:%lu \n", __func__,i,batch_cnt,ctxlist[0][0]->reqlist[0],ctxlist[0][0]->numreq,swapspace_index_first);
	            prep_dynMR_write(RDMABOX_sess, ctxlist[i], replica_cb_list_first, swapspace_index_first, cpuhint, start_idx_tmp_first, debug);
	        }else{
                    printk("rdmabox[%s]: batch_cnt:%d/%d preMR prep write. req:%p numreq:%d swapspace:%lu \n", __func__,i,batch_cnt,ctxlist[0][0]->reqlist[0],ctxlist[0][0]->numreq,swapspace_index_first);
                    prep_preMR_write(RDMABOX_sess, ctxlist[i], replica_cb_list_first, swapspace_index_first, cpuhint, start_idx_tmp_first, 0);
	        }

                //chaining
		if(i == batch_cnt - 1){
	            ctxlist[i][0]->rdma_sq_wr.next = NULL;
		}else{
	            ctxlist[i][0]->rdma_sq_wr.next = &ctxlist[i+1][0]->rdma_sq_wr;
		}
	    }
	} // batch_cnt

        // posting
	for(j=0; j<NUM_REPLICA; j++){
                //printk("rdmabox[%s] post write swapspace_index:%d \n", __func__,swapspace_index);
                err = ib_post_send(replica_cb_list_first[j]->qplist[cpuhint], &ctxlist[0][j]->rdma_sq_wr, &bad_wr);
                if (err) {
                    printk("rdmabox[%s]:client post write error rst=%d\n",__func__, err);
                    return 1;
                 }
	}

	if(!tmp){
    	    //must_go = 0;
            put_cpu();
	    return 0;
	}else{
	    must_go = 1;
	    goto reload;
	}

    }//while 

    put_cpu();
    return 0;  
}
*/

// SGE batch + doorbell
int new_hybrid_batch_write_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
    struct request *tmp_first=NULL;
    int err;
    int i,j;
    struct rdma_ctx *ctx[DOORBELL_BATCH_SIZE];
    int batch_cnt=0;
    int first_pos=0;
    int swapspace_index, swapspace_index_first;
    int boundary_index;
    unsigned long start_idx_tmp, start_tmp;
    unsigned long start_idx_tmp_first, start_tmp_first;
    struct kernel_cb *cb;
    struct remote_chunk_g *chunk;
    int chunk_index;
    struct kernel_cb *replica_cb_list[NUM_REPLICA];
    struct kernel_cb *replica_cb_list_first[NUM_REPLICA];
    int must_go = 0;
    int debug=0;
    int mode;
    struct ib_send_wr * bad_wr;
    int cur_offset;

#ifdef FLOW_CONTROL
retry_post:
    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	goto retry_post;
    }else{
	atomic_inc(&num_outstanding);
#endif
	get_cpu();
	while(batch_cnt < DOORBELL_BATCH_SIZE){
	    struct request *tmp;
	    err = get_req_item(&tmp);
	    if(err){
		if(tmp_first){	
		    //printk("rdmabox[%s]%s: no further item. go send\n", __func__,__LINE__);
		    tmp = NULL;
		    goto send;
		}
      	        //printk("rdmabox[%s]%s: no item. return\n", __func__,__LINE__);
		put_cpu();
		return 0;
	    }

	    start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT; // 3
	    start_tmp = start_idx_tmp << PAGE_SHIFT;
	    swapspace_index = start_tmp >> ONE_GB_SHIFT;

	    // check two chunk
	    /*
	    boundary_index = (start_tmp + (tmp->nr_phys_segments*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;
	    if (swapspace_index != boundary_index) {
		printk("rdmabox[%s]%s: tmp has two chunk. \n", __func__,__LINE__);
		radix_write(RDMABOX_sess,tmp);
		continue;
	    }
	    */

	    //printk("[%s]: Incoming Req:%p start_idx_tmp=%lu nr_segs=%d \n", __func__,tmp,start_idx_tmp, tmp->nr_phys_segments);

	    //find primary cb
	    cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
	    //printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
	    if(!cb){
		printk("[%s]: cb is null \n", __func__);
		put_cpu();
		return 1;
	    }

	    //find primary chunk
	    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	    chunk = replica_cb_list[0]->remote_chunk.chunk_list[chunk_index];
/*
	    // check if READONLY
	    if(chunk->chunk_state >= READONLY){
	    	printk("rdmabox[%s]: this chunk is READONLY. MIGRATION on progress\n",__func__);
try_again:
   	    	err = put_req_item(tmp);
	    	if(err){
		    printk("[%s]: Fail to put req item. retry \n", __func__);
		    goto try_again;
	        }
		continue;
	    }
*/ 
	    if (tmp_first && swapspace_index_first != swapspace_index) {
		printk("rdmabox[%s]%s: tmp has different swapspace_index \n", __func__,__LINE__);
		goto send;
	    }

	if(!tmp_first)
	{
reload:
	    tmp_first=tmp;
	    swapspace_index_first = swapspace_index;
	    for (i=0; i<NUM_REPLICA; i++){
		replica_cb_list_first[i] = replica_cb_list[i];
	    }
	    start_idx_tmp_first = start_idx_tmp;
	    first_pos = batch_cnt;

	    get_single(RDMABOX_sess, &ctx[first_pos], NULL, replica_cb_list_first[0]->cb_index, cpuhint, 1, 0, 1);
	    
	    ctx[first_pos]->numreq = 0;
	    atomic_set(&ctx[first_pos]->ref_count,0);
 	    cur_offset = tmp_first->nr_phys_segments;
            ctx[first_pos]->total_segments = cur_offset;
	    ctx[first_pos]->reqlist[ctx[first_pos]->numreq++] = tmp_first;
	    ctx[first_pos]->len = tmp_first->nr_phys_segments * RDMABOX_PAGE_SIZE;
	    batch_cnt++;
	    //printk("rdmabox[%s]: set tmp_first req:%p ctx[%d]=%p \n", __func__,tmp,first_pos, ctx[first_pos]);

	    if(must_go){
	        //printk("rdmabox[%s]: must_go tmp_first req:%p ctx[%d]=%p \n", __func__,tmp,first_pos, ctx[first_pos]);
		tmp = NULL;
		goto send;
	    }else{
	        //printk("rdmabox[%s]: continue tmp_first req:%p ctx[%d]=%p \n", __func__,tmp,first_pos, ctx[first_pos]);
		continue;
   	    }
	}

	// check if it is next contiguous item
	// check if it fits into one item
        if( (start_idx_tmp == start_idx_tmp_first + cur_offset) &&
		(cur_offset + tmp->nr_phys_segments <= MAX_SGE) )
	{
	    //FIXME: null crash when merging
	    //printk("rdmabox[%s]: potential sge merge. req:%p start_idx_tmp:%lu nr_segs:%d numreq:%d tmp_first_nr_seg:%d \n", __func__,tmp, start_idx_tmp,tmp->nr_phys_segments,ctx[first_pos]->numreq,tmp_first->nr_phys_segments);
	    /*
	        cur_offset = cur_offset + tmp->nr_phys_segments;
                ctx[first_pos]->total_segments = cur_offset;
	        ctx[first_pos]->reqlist[ctx[first_pos]->numreq++] = tmp;
	        ctx[first_pos]->len = ctx[first_pos]->len + (tmp->nr_phys_segments * RDMABOX_PAGE_SIZE);
	        printk("rdmabox[%s]: merge. req:%p start_idx_tmp:%lu nr_segs:%d numreq:%d tmp_first_nr_seg:%d \n", __func__,tmp, start_idx_tmp,tmp->nr_phys_segments,ctx[first_pos]->numreq,tmp_first->nr_phys_segments);
	        debug=1;
                continue;
   	    */
        }
	// if gets here, fail to preMR or dynMR batch, now we do doorbell batch
        // for now, no replica support
        get_single(RDMABOX_sess, &ctx[batch_cnt], NULL, replica_cb_list[0]->cb_index, cpuhint, 1, 0, 1);

        ctx[batch_cnt]->numreq = 0;
        atomic_set(&ctx[batch_cnt]->ref_count,0);
        ctx[batch_cnt]->total_segments = tmp->nr_phys_segments;
	ctx[batch_cnt]->reqlist[ctx[batch_cnt]->numreq++] = tmp;
	ctx[batch_cnt]->len = tmp->nr_phys_segments * RDMABOX_PAGE_SIZE;
	batch_cnt++;
/*
	printk("rdmabox[%s]: doorbell merge req:%p ctx[%d]=%p ctx_first[0]=%p\n", __func__,tmp, batch_cnt-1, ctx[batch_cnt-1], ctx[0]);
        for(i=0; i<batch_cnt; i++){
                printk("rdmabox[%s]: batch_cnt:%d/%d req:%p numreq:%d ctx[%d]=%p \n", __func__,i,batch_cnt,ctx[i]->reqlist[0],ctx[i]->numreq,i, ctx[i]);
	}
*/
        continue;
send:
        if(batch_cnt == 1){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
            ctx[0]->rdma_sq_wr.wr.next = NULL;
#else
            ctx[0]->rdma_sq_wr.next = NULL;
#endif
            //printk("rdmabox[%s]: single dynMR prep write. req:%p numreq:%d swapspace:%lu \n", __func__,ctx[0]->reqlist[0], ctx[0]->numreq, swapspace_index_first);
	    prep_dynMR_write(RDMABOX_sess, ctx[0], replica_cb_list_first, swapspace_index_first, cpuhint, start_idx_tmp_first, debug);
	}else{
	    for(i=0; i<batch_cnt; i++){
                //printk("rdmabox[%s]: batch_cnt:%d/%d dynMR prep write. req:%p numreq:%d ctx[%d]=%p \n", __func__,i,batch_cnt,ctx[i]->reqlist[0],ctx[i]->numreq,batch_cnt, ctx[i]);
                prep_dynMR_write(RDMABOX_sess, ctx[i], replica_cb_list_first, swapspace_index_first, cpuhint, start_idx_tmp_first, debug);

                //chaining
		if(i == batch_cnt - 1){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
                    ctx[i]->rdma_sq_wr.wr.next = NULL;
#else
	            ctx[i]->rdma_sq_wr.next = NULL;
#endif
		}else{
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	            ctx[i]->rdma_sq_wr.wr.next = &ctx[i+1]->rdma_sq_wr;
#else
	            ctx[i]->rdma_sq_wr.next = &ctx[i+1]->rdma_sq_wr;
#endif
		}
	    }
	} // batch_cnt

	for(j=0; j<NUM_REPLICA; j++){
                //printk("rdmabox[%s] post write swapspace_index:%d \n", __func__,swapspace_index);
                err = ib_post_send(replica_cb_list_first[j]->qplist[cpuhint], &ctx[0]->rdma_sq_wr, &bad_wr);
                if (err) {
                    printk("rdmabox[%s]:client post write error rst=%d\n",__func__, err);
                    return 1;
                 }
	}
	batch_cnt=0;

	if(!tmp){
            //printk("rdmabox[%s] return \n", __func__);
  	    put_cpu();
	    return 0;
	}else{
            //printk("rdmabox[%s] reload with req:%p \n", __func__,tmp);
	    must_go = 1;
	    goto reload;
	}

    }//while 

    put_cpu();
    return 0;  
}


//int doorbell_batch_write_fn(void *arg)
int doorbell_batch_write_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
	//struct RDMABOX_session *RDMABOX_sess = arg;
	//int cpuhint = 0;
	struct request *tmp_first=NULL;
	int err;
	int i,j;
	struct rdma_ctx *ctx[DOORBELL_BATCH_SIZE];
	struct rdma_mr *mr;
	struct bio *tmpbio;
	char *buffer;
	int swapspace_index, swapspace_index_first;
	int boundary_index;
	unsigned long start_idx_tmp, start_tmp;
	unsigned long start_idx_tmp_first, start_tmp_first;
	struct kernel_cb *cb;
	struct remote_chunk_g *chunk;
	int chunk_index, chunk_index_first;
	struct kernel_cb *replica_cb_list[NUM_REPLICA];
	struct kernel_cb *replica_cb_list_first[NUM_REPLICA];
	unsigned char *cmem;
	//unsigned long offset, offset_first;
	int must_go = 0;

	//msleep(5000);
	//printk("rdmabox[%s]%s: thread starts \n", __func__,__LINE__);

#ifdef FLOW_CONTROL
retry_post:
	if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	    goto retry_post;
	}else{
	    atomic_inc(&num_outstanding);
#endif
         get_cpu();
	    while(true){
		struct request *tmp;
		err = get_req_item(&tmp);
		if(err){
		    if(tmp_first){	
			must_go=1;
			goto send;
		    }
		    //msleep(100);
		    //continue;
                    put_cpu();
		    return 0;
		}

		// check two chunk
		start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT; // 3
		start_tmp = start_idx_tmp << PAGE_SHIFT;
		swapspace_index = start_tmp >> ONE_GB_SHIFT;
		boundary_index = (start_tmp + (tmp->nr_phys_segments*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;

		if (swapspace_index != boundary_index) {
		    printk("rdmabox[%s]%s: tmp has two chunk. \n", __func__,__LINE__);
		    radix_write(RDMABOX_sess,tmp);
                    put_cpu();
		    continue;
		}

		//offset = start_tmp & ONE_GB_MASK;	

		//find primary cb
		cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
		//printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
		if(!cb){
		    printk("[%s]: cb is null \n", __func__);
                    put_cpu();
		    continue;
		}

		//find primary chunk
		chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
		chunk = replica_cb_list[0]->remote_chunk.chunk_list[chunk_index];

		if (tmp_first && swapspace_index_first != swapspace_index) {
		    //printk("[%s]: Incoming Req:%x different swapspace_first:%d  swapspace:%d\n", __func__,tmp, tmp->nr_phys_segments, swapspace_index_first, swapspace_index);
		    goto send;
		}

		if(!tmp_first)
		{
reload:
		    tmp_first=tmp;
		    tmpbio = tmp_first->bio;
		    //offset_first = offset;
		    chunk_index_first = chunk_index;
		    swapspace_index_first = swapspace_index;
		    for (i=0; i<NUM_REPLICA; i++){
			replica_cb_list_first[i] = replica_cb_list[i];
		    }
		    start_idx_tmp_first = start_idx_tmp;
#if defined(USE_DOORBELL_WRITE)
		    // get prime ctx and mr
		    get_both(RDMABOX_sess, &ctx[0], &mr, replica_cb_list_first[0]->cb_index, cpuhint, 1, 1, 1);
		    ctx[0]->mr = mr;
		    ctx[0]->numreq = 0;
		    atomic_set(&ctx[0]->ref_count,0);

		    for (i=0; i < tmp_first->nr_phys_segments;){
			buffer = bio_data(tmpbio);
			j = tmpbio->bi_phys_segments;
			memcpy(ctx[0]->mr->rdma_buf + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
			i += j;
			tmpbio = tmpbio->bi_next;
		    }
#elif defined(USE_DOORBELL_WRITE_SGE)
		    // get prime ctx 
		    get_single(RDMABOX_sess , &ctx[0], NULL, replica_cb_list_first[0]->cb_index, cpuhint,1, 0, 1);
		    ctx[0]->numreq = 0;
		    atomic_set(&ctx[0]->ref_count,0);
#endif
		    ctx[0]->reqlist[ctx[0]->numreq++] = tmp_first;
		    ctx[0]->len = tmp_first->nr_phys_segments * RDMABOX_PAGE_SIZE;

		    if(must_go){
			goto send;
		    }else{
			continue;
		    }
		}

		if(ctx[0]->numreq < DOORBELL_BATCH_SIZE)
		{
#if defined(USE_DOORBELL_WRITE)
		    tmpbio = tmp->bio;
		    get_both(RDMABOX_sess, &ctx[ctx[0]->numreq], &mr, replica_cb_list_first[0]->cb_index, cpuhint, 1, 1, 1);
		    ctx[ctx[0]->numreq]->mr = mr;
		    // merge items
		    for(i=0; i < tmp->nr_phys_segments ;){
			buffer = bio_data(tmpbio);
			j = tmpbio->bi_phys_segments;
			memcpy(ctx[ctx[0]->numreq]->mr->rdma_buf + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
			i += j;
			tmpbio = tmpbio->bi_next;
		    }
#elif defined(USE_DOORBELL_WRITE_SGE)
		    get_single(RDMABOX_sess , &ctx[ctx[0]->numreq], NULL, replica_cb_list_first[0]->cb_index, cpuhint,1, 0, 1);
#endif
                    ctx[ctx[0]->numreq]->numreq = 0;
		    atomic_set(&ctx[ctx[0]->numreq]->ref_count,0);
		    ctx[ctx[0]->numreq]->len = tmp->nr_phys_segments * RDMABOX_PAGE_SIZE;
		    ctx[ctx[0]->numreq++]->reqlist[0] = tmp;

		    //printk("rdmabox[%s]: merge. start_idx_tmp:%lu nr_segs:%d numreq:%d \n", __func__,start_idx_tmp,tmp->nr_phys_segments,ctx[0]->numreq);
		    continue;
		}
send:
		//printk("rdmabox[%s]: doorbell remote write. numreq:%d start_idx_tmp_first=%lu nr_seg:%d swapspace:%lu \n", __func__,ctx[0]->numreq,start_idx_tmp_first,(ctx[0]->len/RDMABOX_PAGE_SIZE),swapspace_index_first);
#if defined(USE_DOORBELL_WRITE)
		err = doorbell_remote_write(RDMABOX_sess, ctx, replica_cb_list_first, swapspace_index_first, cpuhint, start_idx_tmp_first, chunk_index_first);
#elif defined(USE_DOORBELL_WRITE_SGE)
		err = doorbell_remote_write_sge(RDMABOX_sess, ctx, replica_cb_list_first, swapspace_index_first, cpuhint, start_idx_tmp_first, chunk_index_first);
#endif
		if(err){
		    printk("rdmabox[%s]: remote write fail\n", __func__);
                    put_cpu();
		    return 1;
		}
		if(must_go){
		    //must_go = 0;
		    //tmp_first=NULL;
		    //continue;
                    put_cpu();
		    return 0;
		}else{
		    must_go = 1;
		    goto reload;
		}

	    }//while 

            put_cpu();
	    return 0;  
}


int batch_write_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
    struct request *tmp_first=NULL;
    int err;
    int i,j;
    struct rdma_ctx *ctx[NUM_REPLICA];
#ifndef SUPPORT_SGE_WRITE_BATCH
    struct rdma_mr *mr;
    struct bio *tmpbio;
    char *buffer;
#endif
    int cur_offset;
    int swapspace_index, swapspace_index_first;
    int boundary_index;
    unsigned long start_idx_tmp, start_tmp;
    unsigned long start_idx_tmp_first, start_tmp_first;
    struct kernel_cb *cb;
    struct remote_chunk_g *chunk;
    int chunk_index;
    struct kernel_cb *replica_cb_list[NUM_REPLICA];
    struct kernel_cb *replica_cb_list_first[NUM_REPLICA];
    unsigned char *cmem;
    unsigned long offset, offset_first;
    int must_go = 0;
    int debug=0;

#ifdef FLOW_CONTROL
retry_post:
    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	goto retry_post;
    }else{
	atomic_inc(&num_outstanding);
#endif

	while(true){
	    struct request *tmp;
	    err = get_req_item(&tmp);
	    if(err){
		if(tmp_first){	
		    tmp = NULL;
		    goto send;
		}
		return 0;
	    }

#if defined(SUPPORT_SGE_WRITE_BATCH)
	     // TODO : FIX sg_next NULL crash issue. For now we use single I/O on write sge batch
            must_go=1;
#endif

	    // check two chunk
	    start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT; // 3
	    start_tmp = start_idx_tmp << PAGE_SHIFT;
	    swapspace_index = start_tmp >> ONE_GB_SHIFT;
	    boundary_index = (start_tmp + (tmp->nr_phys_segments*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;

	    if (swapspace_index != boundary_index) {
		printk("rdmabox[%s]%s: tmp has two chunk. \n", __func__,__LINE__);
		radix_write(RDMABOX_sess,tmp);
		continue;
	    }

	    offset = start_tmp & ONE_GB_MASK;	
	    //printk("[%s]: Incoming Req:%p start_idx_tmp=%lu nr_segs=%d \n", __func__,tmp,start_idx_tmp, tmp->nr_phys_segments);

	    //find primary cb
	    cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
	    //printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
	    if(!cb){
		printk("[%s]: cb is null \n", __func__);
		return 1;
	    }

	    //find primary chunk
	    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	    chunk = replica_cb_list[0]->remote_chunk.chunk_list[chunk_index];

	    // check if READONLY
	    if(chunk->chunk_state >= READONLY){
	    	printk("rdmabox[%s]: this chunk is READONLY. MIGRATION on progress\n",__func__);
try_again:
   	    	err = put_req_item(tmp);
	    	if(err){
		    printk("[%s]: Fail to put req item. retry \n", __func__);
		    goto try_again;
	        }
		continue;
	    }
 
	    if (tmp_first && swapspace_index_first != swapspace_index) {
		printk("rdmabox[%s]%s: tmp has different swapspace_index \n", __func__,__LINE__);
		goto send;
	    }

	if(!tmp_first)
	{
reload:
	    tmp_first=tmp;
#ifndef SUPPORT_SGE_WRITE_BATCH
	    tmpbio = tmp_first->bio;
#endif
	    cur_offset = 0;
	    offset_first = offset;
	    swapspace_index_first = swapspace_index;
	    for (i=0; i<NUM_REPLICA; i++){
		replica_cb_list_first[i] = replica_cb_list[i];
	    }
	    start_idx_tmp_first = start_idx_tmp;

#if defined(SUPPORT_SGE_WRITE_BATCH)
	    get_single(RDMABOX_sess, &ctx[0], NULL, replica_cb_list_first[0]->cb_index, cpuhint, 1, 0, 1);
#else
	    // get prime ctx and mr
	    get_both(RDMABOX_sess, &ctx[0], &mr, replica_cb_list_first[0]->cb_index, cpuhint, 1, 1, 1);
	    ctx[0]->mr = mr;

	    for (i=0; i < tmp_first->nr_phys_segments;){
		buffer = bio_data(tmpbio);
		j = tmpbio->bi_phys_segments;
		memcpy(ctx[0]->mr->rdma_buf + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
		i += j;
		tmpbio = tmpbio->bi_next;
   	    }
#endif
	    ctx[0]->numreq = 0;
	    atomic_set(&ctx[0]->ref_count,0);
 	    cur_offset = tmp_first->nr_phys_segments;
#if defined(SUPPORT_SGE_WRITE_BATCH)
	    ctx[0]->total_segments = cur_offset;
#endif
	    ctx[0]->reqlist[ctx[0]->numreq++] = tmp_first;
	    ctx[0]->len = tmp_first->nr_phys_segments * RDMABOX_PAGE_SIZE;

	    if(must_go){
		tmp = NULL;
		goto send;
	    }else{
		continue;
   	    }
	}

	// check if it is next contiguous item
	// check if it fits into one item
#ifdef SUPPORT_SGE_WRITE_BATCH
	if( (start_idx_tmp == start_idx_tmp_first + cur_offset) &&
		(cur_offset + tmp->nr_phys_segments <= MAX_SGE) )
#else
	if( (start_idx_tmp == start_idx_tmp_first + cur_offset) &&
		(cur_offset + tmp->nr_phys_segments <= RDMA_WR_BUF_LEN)  )
#endif
	{
#ifndef SUPPORT_SGE_WRITE_BATCH
       	    tmpbio = tmp->bio;
	    // merge items
	    for(i=0; i < tmp->nr_phys_segments ;){
		buffer = bio_data(tmpbio);
		j = tmpbio->bi_phys_segments;
		memcpy(ctx[0]->mr->rdma_buf + ctx[0]->len + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
		i += j;
		tmpbio = tmpbio->bi_next;
    	    }
#endif
	    cur_offset = cur_offset + tmp->nr_phys_segments;
#if defined(SUPPORT_SGE_WRITE_BATCH)
            ctx[0]->total_segments = cur_offset;
#endif
	    ctx[0]->reqlist[ctx[0]->numreq++] = tmp;
	    ctx[0]->len = ctx[0]->len + (tmp->nr_phys_segments * RDMABOX_PAGE_SIZE);
	    printk("rdmabox[%s]: merge. req:%p start_idx_tmp:%lu nr_segs:%d numreq:%d tmp_first_nr_seg:%d \n", __func__,tmp, start_idx_tmp,tmp->nr_phys_segments,ctx[0]->numreq,tmp_first->nr_phys_segments);
	    debug=1;

	    continue;
	}

send:
	//printk("rdmabox[%s]: remote write. numreq:%d start_idx_tmp_first=%lu nr_seg:%d swapspace:%lu \n", __func__,ctx[0]->numreq,start_idx_tmp_first,(ctx[0]->len/RDMABOX_PAGE_SIZE),swapspace_index_first);
	//printk("rdmabox[%s]: remote write. numreq:%d start_idx_tmp_first=%lu nr_seg:%d swapspace:%lu \n", __func__,ctx[0]->numreq,start_idx_tmp_first,ctx[0]->total_segments,swapspace_index_first);
	err = remote_write(RDMABOX_sess, ctx, replica_cb_list_first, swapspace_index_first, offset_first, cpuhint, start_idx_tmp_first, debug);
	if(err){
	    printk("rdmabox[%s]: remote write fail\n", __func__);
	    return 1;
	}
	if(!tmp){
    	    //must_go = 0;
	    return 0;
	}else{
	    must_go = 1;
	    goto reload;
	}

    }//while 

    return 0;  
}

int request_sge_wr(struct RDMABOX_session *RDMABOX_sess, struct request *req, struct rdma_ctx **ctx, struct kernel_cb **replica_cb_list, int cpuhint)
{
    int retval = 0;
    int swapspace_index;
    int boundary_index;
    unsigned long offset;
    int i,j,k;
    int ret;
    struct bio *tmpbio = req->bio;
    unsigned int nr_seg = req->nr_phys_segments;

    struct scatterlist *sg;
    struct sg_table *sgt;
    int nents;
    size_t start = blk_rq_pos(req) << SECTOR_SHIFT;

    swapspace_index = start >> ONE_GB_SHIFT;
    offset = start & ONE_GB_MASK;

    get_cpu();

    sgt = &ctx[0]->data_tbl;
    sgt->sgl =  ctx[0]->sgl;
    sg = sgt->sgl;

    sg_init_table( ctx[0]->data_tbl.sgl, req->nr_phys_segments);
    retval = req_map_sg(req,  ctx[0]);
    if (retval) {
	printk("failed to map sg\n");
	put_cpu();
	return 1;
    }
    nents = dma_map_sg(RDMABOX_sess->pd->device->dma_device, sgt->sgl, sgt->nents, DMA_TO_DEVICE);
    if (!nents) {
	sg_mark_end(sg);
	sgt->nents = sgt->orig_nents;
	put_cpu();
	return -1;
    }

    for (i = 0; i < nents; i++) {
	ctx[0]->rdma_sgl_wr[i].addr   = ib_sg_dma_address(RDMABOX_sess->pd->device, sg);
	ctx[0]->rdma_sgl_wr[i].length = ib_sg_dma_len(RDMABOX_sess->pd->device, sg);
	ctx[0]->rdma_sgl_wr[i].lkey = replica_cb_list[0]->qp[0]->device->local_dma_lkey;
	sg = sg_next(sg);
    }

    for(j=0; j<NUM_REPLICA; j++){
	if(j>0){
	    get_single(RDMABOX_sess, &ctx[j], NULL, replica_cb_list[j]->cb_index, cpuhint, 1, 0, 1);
	}//j>0
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	ctx[j]->rdma_sq_wr.wr.sg_list = &ctx[0]->rdma_sgl_wr[0];
	ctx[j]->rdma_sq_wr.wr.num_sge = nents;
#else
	ctx[j]->rdma_sq_wr.sg_list = &ctx[0]->rdma_sgl_wr[0];
	ctx[j]->rdma_sq_wr.num_sge = nents;
#endif
	atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	ctx[j]->prime_ctx = ctx[0];
	ctx[j]->sending_item = NULL;
	ctx[j]->replica_index = j;
	ctx[j]->reqlist[0] = req;
	ctx[j]->numreq = 1;
	ctx[j]->isSGE = 1;
	ctx[j]->len = nr_seg * RDMABOX_PAGE_SIZE;
    }//replica forloop

#ifdef DISKBACKUP
    stackbd_bio_generate_req(ctx[0], req);
#endif

    // send out
    //ret = __rdma_write(RDMABOX_sess, replica_cb_list[0], swapspace_index, offset, 0, ctx[0], cpuhint%NUM_CHANNEL);
    ret = __rdma_write(RDMABOX_sess, replica_cb_list[0], swapspace_index, offset, 0, ctx[0], cpuhint);
    if (ret){
	put_cpu();
	return ret;
    }
    for(j=1; j<NUM_REPLICA; j++){
	//ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint%NUM_CHANNEL);
	ret =  __rdma_write(RDMABOX_sess, replica_cb_list[j], swapspace_index, offset, j, ctx[j], cpuhint);
	if (ret){
	    put_cpu();
	    return ret;
	}
    }

    put_cpu();
    return 0;
}


int hybrid_batch_write_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint)
{
    struct request *tmp_first=NULL;
    int err;
    int i,j;
    struct rdma_ctx *ctx[NUM_REPLICA];
    struct rdma_mr *mr;
    struct bio *tmpbio;
    char *buffer;
    int cur_offset;
    int swapspace_index, swapspace_index_first;
    int boundary_index;
    unsigned long start_idx_tmp, start_tmp;
    unsigned long start_idx_tmp_first, start_tmp_first;
    struct kernel_cb *cb;
    struct remote_chunk_g *chunk;
    int chunk_index;
    struct kernel_cb *replica_cb_list[NUM_REPLICA];
    struct kernel_cb *replica_cb_list_first[NUM_REPLICA];
    unsigned char *cmem;
    unsigned long offset, offset_first;
    int must_go = 0;
    int isSGE;

#ifdef FLOW_CONTROL
retry_post:
    if(atomic_read(&num_outstanding) >= MAX_NUM_OUTSTANDING){
	goto retry_post;
    }else{
	atomic_inc(&num_outstanding);
#endif

	get_cpu();
	while(true){
	    struct request *tmp;
	    err = get_req_item(&tmp);
	    if(err){
		if(tmp_first){	
		    tmp = NULL;
		    goto send;
		}
		put_cpu();
		return 0;
	    }
	    /*
#ifdef SUPPORT_SGE_WRITE_BATCH
if(tmp->nr_phys_segments == MAX_SECTORS){
	    //printk("rdmabox[%s]%s: SGE. start_idx:%lu nr_phy:%d \n", __func__,__LINE__,start_idx_tmp,tmp->nr_phys_segments);
	    request_write(RDMABOX_sess,tmp,cpuhint);
	    continue;
	    }
#endif
	     */
	    // check two chunk
	    start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT; // 3
	    start_tmp = start_idx_tmp << PAGE_SHIFT;
	    swapspace_index = start_tmp >> ONE_GB_SHIFT;
	    boundary_index = (start_tmp + (tmp->nr_phys_segments*RDMABOX_PAGE_SIZE) - RDMABOX_PAGE_SIZE) >> ONE_GB_SHIFT;

	    if (swapspace_index != boundary_index) {
		printk("rdmabox[%s]%s: tmp has two chunk. \n", __func__,__LINE__);
		radix_write(RDMABOX_sess,tmp);
		continue;
	    }

	    offset = start_tmp & ONE_GB_MASK;	
	    //printk("[%s]: Incoming Req:%x start_idx_tmp=%lu nr_segs=%d \n", __func__,tmp,start_idx_tmp, tmp->nr_phys_segments);

	    //find primary cb
	    cb = get_cb(RDMABOX_sess, swapspace_index, replica_cb_list);
	    //printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
	    if(!cb){
		printk("[%s]: cb is null \n", __func__);
		return 1;
	    }

	    //find primary chunk
	    chunk_index = RDMABOX_sess->mapping_swapspace_to_chunk[0][swapspace_index];
	    chunk = replica_cb_list[0]->remote_chunk.chunk_list[chunk_index];
	    /*
	    // check if READONLY
	    if(chunk->chunk_state >= READONLY){
	    printk("rdmabox[%s]: this chunk is READONLY. MIGRATION on progress\n",__func__);
try_again:
err = put_req_item(tmp);
if(err){
printk("[%s]: Fail to put req item. retry \n", __func__);
goto try_again;
}
continue;
}
	     */ 
	    if (tmp_first && swapspace_index_first != swapspace_index) {
		goto send;
	    }

if(!tmp_first)
{
reload:
    tmp_first=tmp;
    tmpbio = tmp_first->bio;
    cur_offset = 0;
    offset_first = offset;
    swapspace_index_first = swapspace_index;
    for (i=0; i<NUM_REPLICA; i++){
	replica_cb_list_first[i] = replica_cb_list[i];
    }
    start_idx_tmp_first = start_idx_tmp;

    // get prime ctx and mr
    isSGE = get_both(RDMABOX_sess, &ctx[0], &mr, replica_cb_list_first[0]->cb_index, cpuhint, 1, 1, 1);
    ctx[0]->isSGE = isSGE;
    ctx[0]->numreq = 0;
    atomic_set(&ctx[0]->ref_count,0);

    //if(isSGE){
    if(tmp_first->nr_phys_segments <= MAX_SGE || isSGE){
	//printk("rdmabox[%s] failed to get MR. Do SGE.\n",__func__);
	ctx[0]->isSGE = 1;
	request_sge_wr(RDMABOX_sess, tmp_first, ctx, replica_cb_list_first, cpuhint);
	put_cpu();
	return 0;
    }

    ctx[0]->mr = mr;

    for (i=0; i < tmp_first->nr_phys_segments;){
	buffer = bio_data(tmpbio);
	j = tmpbio->bi_phys_segments;
	memcpy(ctx[0]->mr->rdma_buf + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
	i += j;
	tmpbio = tmpbio->bi_next;
    }
    cur_offset = tmp_first->nr_phys_segments;
    ctx[0]->reqlist[ctx[0]->numreq++] = tmp_first;
    ctx[0]->len = tmp_first->nr_phys_segments * RDMABOX_PAGE_SIZE;

    if(must_go){
	tmp = NULL;
	goto send;
    }else{
	continue;
    }
}

if(tmp->nr_phys_segments <= MAX_SGE){
    must_go=1;
    goto send;
}

// check if it is next contiguous item
// check if it fits into one item
if( (start_idx_tmp == start_idx_tmp_first + cur_offset) &&
	(cur_offset + tmp->nr_phys_segments <= RDMA_WR_BUF_LEN) )
{
    tmpbio = tmp->bio;
    // merge items
    for(i=0; i < tmp->nr_phys_segments ;){
	buffer = bio_data(tmpbio);
	j = tmpbio->bi_phys_segments;
	memcpy(ctx[0]->mr->rdma_buf + ctx[0]->len + (i * RDMABOX_PAGE_SIZE), buffer, RDMABOX_PAGE_SIZE * j);
	i += j;
	tmpbio = tmpbio->bi_next;
    }
    cur_offset = cur_offset + tmp->nr_phys_segments;
    ctx[0]->reqlist[ctx[0]->numreq++] = tmp;
    ctx[0]->len = ctx[0]->len + (tmp->nr_phys_segments * RDMABOX_PAGE_SIZE);
    //printk("rdmabox[%s]: merge. start_idx_tmp:%lu nr_segs:%d numreq:%d \n", __func__,start_idx_tmp,tmp->nr_phys_segments,numreq);
    continue;
}

send:
//printk("rdmabox[%s]: remote write. numreq:%d start_idx_tmp_first=%lu nr_seg:%d swapspace:%lu \n", __func__,numreq,start_idx_tmp_first,(ctx->len/RDMABOX_PAGE_SIZE),swapspace_index_first);
err = remote_write(RDMABOX_sess, ctx, replica_cb_list_first, swapspace_index_first, offset_first, cpuhint, start_idx_tmp_first, 0);
if(err){
    printk("rdmabox[%s]: remote write fail\n", __func__);
    panic("remote write fail");
}
if(!tmp){
    must_go = 0;
    put_cpu();
    return 0;
}else{
    must_go = 1;
    goto reload;
}

}//while 

put_cpu();
return 0;  
}

//eviction
static int evict_handler(void *data)
{
    struct kernel_cb *old_cb = data;	
    struct kernel_cb *new_cb = NULL;
    int client_evict_chunk_index;
    int src_cb_index;
    int src_chunk_index;
    int dest_cb_index;
    int dest_chunk_index;
    int replica_index;
    int i,j;
    int err = 0;
    struct RDMABOX_session *RDMABOX_sess = old_cb->RDMABOX_sess;
    // this is need for eviction
    //int *cb_chunk_map = old_cb->remote_chunk.chunk_map;
    //int evict_list[STACKBD_SIZE_G]; //session chunk index
    //int sess_chunk_index;

    int chunk_index;
    unsigned long wr_sum=0;
    unsigned long wr_sum_prev=0;
    unsigned long rd_sum=0;
    unsigned long rd_sum_prev=0;

    while (old_cb->state != ERROR) {
#ifdef DEBUG
	printk("rdmabox[%s]: evict handler go to sleep and wait...\n", __func__);
#endif
	/*  
	    printk("rdmabox[%s]: wait for C_QUERY \n", __func__);
	    wait_event_interruptible(old_cb->remote_chunk.sem, (old_cb->remote_chunk.c_state == C_QUERY));	
	    for (i=0; i<MAX_MR_SIZE_GB; i++) {
	    if(old_cb->recv_buf.rkey[i] != 0 ){
	    old_cb->send_buf.rkey[i] = 1;
	    chunk_index = ntohl(old_cb->recv_buf.rkey[i]);
	    wr_sum_prev = RDMABOX_sess->wr_ops[chunk_index][0];
	    rd_sum_prev = RDMABOX_sess->rd_ops[chunk_index][0];

	    msleep(2000);

	    wr_sum = RDMABOX_sess->wr_ops[chunk_index][0];
	    rd_sum = RDMABOX_sess->rd_ops[chunk_index][0];

	    old_cb->send_buf.buf[i] = htonll(wr_sum + rd_sum - rd_sum_prev - wr_sum_prev);
	    printk("rdmabox[%s]: RDMABOX_check_activity swapspace[%d] RD activity:%lu, WR activity:%lu \n",__func__,chunk_index, (rd_sum-rd_sum_prev), (wr_sum-wr_sum_prev));
	    }else{
	    old_cb->send_buf.rkey[i] = 0;
	    old_cb->send_buf.buf[i] = 0;
	    }
	    }
	    old_cb->send_buf.type = RESP_ACTIVITY;
	    post_send(old_cb);
	 */
	//printk("rdmabox[%s]: wait for C_EVICT \n", __func__);
	wait_event_interruptible(old_cb->remote_chunk.sem, (old_cb->remote_chunk.c_state == C_EVICT));	

	// client chunk index
	replica_index = old_cb->remote_chunk.replica_index;
	client_evict_chunk_index = old_cb->remote_chunk.client_swapspace_index;
	src_cb_index = old_cb->remote_chunk.src_cb_index;
	src_chunk_index = old_cb->remote_chunk.src_chunk_index;
	printk("rdmabox[%s]: received EVICT message from node %d \n", __func__,src_cb_index);

	// TODO: comment this for measurement
	//RDMABOX_check_activity(RDMABOX_sess, client_evict_chunk_index);

	// make a decision
	//  TODO : for now default migration

	// 1.migration
	printk("rdmabox[%s]: Set CB_EVICTING state to node %d \n", __func__,src_cb_index);
	RDMABOX_sess->cb_state_list[src_cb_index] = CB_EVICTING;

	// stop write to server
	printk("rdmabox[%s]: set client swapspace[%d] NONE_ALLOWED \n", __func__,client_evict_chunk_index);
	set_chunk_state(old_cb, client_evict_chunk_index, NONE_ALLOWED);

	// remote allocation for migration destination
	// search new destination

	// TODO check write only ops
	printk("rdmabox[%s]: fail to find the migration destination. Send Delete. \n", __func__);
	printk("rdmabox[%s]: wait inflight read, write ops.. \n", __func__);
	RDMABOX_chunk_wait_in_flight_requests(RDMABOX_sess, old_cb, old_cb->remote_chunk.chunk_list[src_chunk_index]->remote_rkey, 0);
	printk("rdmabox[%s]: done inflight read, write ops.. \n", __func__);

	// now send delete chunk msg to server
	for (i=0; i<MAX_MR_SIZE_GB; i++) {
	    if(old_cb->remote_chunk.migration_chunk_map[i] == 'm'){
		old_cb->send_buf.rkey[i] = 1; 
	    }else{
		old_cb->send_buf.rkey[i] = 0; 
	    }
	}

	old_cb->send_buf.type = DELETE_CHUNK;
	post_send(old_cb);

	printk("rdmabox[%s]: wait C_DELETE_CHUNK \n", __func__);
	wait_event_interruptible(old_cb->remote_chunk.sem, (old_cb->remote_chunk.c_state == C_DELETE_CHUNK));	
	printk("rdmabox[%s]: C_DELETE_CHUNK done \n", __func__);

	// clean chunk info from old_cb
	reset_migration_info(old_cb, NULL, replica_index, client_evict_chunk_index);

	set_chunk_state(old_cb, client_evict_chunk_index, INACTIVE);

	continue;

    }//while

    return err;
}


//migration

static int migration_handler(void *data)
{
    struct kernel_cb *old_cb = data;	
    struct kernel_cb *new_cb = NULL;
    int client_evict_chunk_index;
    int src_cb_index;
    int src_chunk_index;
    int dest_cb_index;
    int dest_chunk_index;
    int replica_index;
    int i,j;
    int err = 0;
    struct RDMABOX_session *RDMABOX_sess = old_cb->RDMABOX_sess;
#ifdef RPC_LAT_DEBUG
    struct timeval start, end, t0, t1;
    unsigned long diff;
#endif

    // this is need for eviction
    //int *cb_chunk_map = old_cb->remote_chunk.chunk_map;
    //int evict_list[STACKBD_SIZE_G]; //session chunk index
    //int sess_chunk_index;

    int chunk_index;
    unsigned long wr_sum=0;
    unsigned long wr_sum_prev=0;
    unsigned long rd_sum=0;
    unsigned long rd_sum_prev=0;

    while (old_cb->state != ERROR) {
#ifdef DEBUG
	printk("rdmabox[%s]: evict handler go to sleep and wait...\n", __func__);
#endif
	//printk("rdmabox[%s]: wait for C_EVICT \n", __func__);
	wait_event_interruptible(old_cb->remote_chunk.sem, (old_cb->remote_chunk.c_state == C_EVICT));	
#ifdef RPC_LAT_DEBUG
	do_gettimeofday(&t0);
#endif
	// client chunk index
	replica_index = old_cb->remote_chunk.replica_index;
	client_evict_chunk_index = old_cb->remote_chunk.client_swapspace_index;
	src_cb_index = old_cb->remote_chunk.src_cb_index;
	src_chunk_index = old_cb->remote_chunk.src_chunk_index;
	printk("rdmabox[%s]: received EVICT message from node %d \n", __func__,src_cb_index);

	// TODO: comment this for measurement
	//RDMABOX_check_activity(RDMABOX_sess, client_evict_chunk_index);

	// make a decision
	//  TODO : for now default migration

	// 1.migration
	printk("rdmabox[%s]: Set CB_MIGRATING state to node %d \n", __func__,src_cb_index);
	RDMABOX_sess->cb_state_list[src_cb_index] = CB_MIGRATING;

	// stop write to server
	printk("rdmabox[%s]: set client swapspace[%d] READONLY \n", __func__,client_evict_chunk_index);
	set_chunk_state(old_cb, client_evict_chunk_index, READONLY);

	// remote allocation for migration destination
	// search new destination

	new_cb = RDMABOX_migration_chunk_map(RDMABOX_sess, client_evict_chunk_index, old_cb, src_cb_index, replica_index); //wait inside until INFO_MIGRATION 
	if(!new_cb){
	    // TODO check write only ops
	    printk("rdmabox[%s]: fail to find the migration destination. Send Delete. \n", __func__);
	    printk("rdmabox[%s]: wait inflight read, write ops.. \n", __func__);
	    RDMABOX_chunk_wait_in_flight_requests(RDMABOX_sess, old_cb, old_cb->remote_chunk.chunk_list[src_chunk_index]->remote_rkey, 0);
	    printk("rdmabox[%s]: done inflight read, write ops.. \n", __func__);

	    // now send delete chunk msg to server
	    for (i=0; i<MAX_MR_SIZE_GB; i++) {
		if(old_cb->remote_chunk.migration_chunk_map[i] == 'm'){
		    old_cb->send_buf.rkey[i] = 1; 
		}else{
		    old_cb->send_buf.rkey[i] = 0; 
		}
	    }
#ifdef RPC_LAT_DEBUG
	    do_gettimeofday(&start);
#endif

	    old_cb->send_buf.type = DELETE_CHUNK;
	    post_send(old_cb);

	    printk("rdmabox[%s]: wait C_DELETE_CHUNK \n", __func__);
	    wait_event_interruptible(old_cb->remote_chunk.sem, (old_cb->remote_chunk.c_state == C_DELETE_CHUNK));	
	    printk("rdmabox[%s]: C_DELETE_CHUNK done \n", __func__);

#ifdef RPC_LAT_DEBUG
	    do_gettimeofday(&end);
	    diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
	    printk("lat DELETE_CHUNK %ld usec node %d\n",diff,src_cb_index);
#endif
	    // clean chunk info from old_cb
	    reset_migration_info(old_cb, NULL, replica_index, client_evict_chunk_index);

	    set_chunk_state(old_cb, client_evict_chunk_index, INACTIVE);

#ifdef RPC_LAT_DEBUG
	    do_gettimeofday(&t1);
	    diff=(t1.tv_sec - t0.tv_sec)*1000000 + (t1.tv_usec - t0.tv_usec);
	    printk("lat migrationdelete exec %ld usec node %d\n",diff,src_cb_index);
#endif

	    continue;
	}

	dest_cb_index = new_cb->remote_chunk.dest_cb_index;
	dest_chunk_index = new_cb->remote_chunk.dest_chunk_index;

	// TODO check write only ops
	printk("rdmabox[%s]: wait inflight write ops.. \n", __func__);
	RDMABOX_chunk_wait_in_flight_requests(RDMABOX_sess, old_cb, old_cb->remote_chunk.chunk_list[src_chunk_index]->remote_rkey, 1);
	printk("rdmabox[%s]: done inflight write ops.. \n", __func__);

	// TODO: comment this for measurement
	//RDMABOX_check_activity(RDMABOX_sess, client_evict_chunk_index);

#ifdef RPC_LAT_DEBUG
	do_gettimeofday(&start);
#endif
	printk("rdmabox[%s]: wait MIGRATION_DONE \n", __func__);
	wait_event_interruptible(old_cb->remote_chunk.sem, (old_cb->remote_chunk.c_state == C_MIGRATION_DONE));	
	// updated destination info in this client
	// now we have all set for new remote destination(copying to new dest is done)
	printk("rdmabox[%s]: received MIGRATION_DONE \n", __func__);
#ifdef RPC_LAT_DEBUG
	do_gettimeofday(&end);
	diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
	printk("lat MIGRATION_DONE %ld usec node %d\n",diff,src_cb_index);
#endif

	// update mr information with new destination
	atomic_set(RDMABOX_sess->cb_index_map[0] + (client_evict_chunk_index), dest_cb_index);
	RDMABOX_sess->mapping_swapspace_to_chunk[0][client_evict_chunk_index]=dest_chunk_index;
	new_cb->remote_chunk.c_state = C_READY;
	memcpy(new_cb->remote_chunk.chunk_list[dest_chunk_index]->bitmap_g,old_cb->remote_chunk.chunk_list[src_chunk_index]->bitmap_g,sizeof(int) * BITMAP_INT_SIZE);
	printk("rdmabox[%s]: update migration info. chunk[%d] on server[%d] is destination.\n", __func__,dest_chunk_index,dest_cb_index);

	// wait for in flight ops to previous evicting server
	RDMABOX_chunk_wait_in_flight_requests(RDMABOX_sess, old_cb, old_cb->remote_chunk.chunk_list[src_chunk_index]->remote_rkey, 0);

	// now send delete chunk msg to server
	for (i=0; i<MAX_MR_SIZE_GB; i++) {
	    if(old_cb->remote_chunk.migration_chunk_map[i] == 'm'){
		old_cb->send_buf.rkey[i] = 1; 
	    }else{
		old_cb->send_buf.rkey[i] = 0; 
	    }
	}

	//#ifdef RPC_LAT_DEBUG
	//    do_gettimeofday(&start);
	//#endif

	old_cb->send_buf.type = DELETE_CHUNK;
	post_send(old_cb);
	wait_event_interruptible(old_cb->remote_chunk.sem, (old_cb->remote_chunk.c_state == C_DELETE_CHUNK));	
	printk("rdmabox[%s]: send C_DELETE_CHUNK done \n", __func__);

	// for now kernel side doesn't have hybrid mode. no need to measure kernel side send wc latency
	//#ifdef RPC_LAT_DEBUG
	//    do_gettimeofday(&end);
	//    diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
	//    printk("lat DELETE_CHUNK %ld usec node %d\n",diff,src_cb_index);
	//#endif

	// clean old_cb new_cb info   
	reset_migration_info(old_cb, new_cb, replica_index, client_evict_chunk_index);
	RDMABOX_sess->cb_state_list[src_cb_index] = CB_MAPPED;

	set_chunk_state(old_cb, client_evict_chunk_index, INACTIVE);

#ifdef RPC_LAT_DEBUG
	do_gettimeofday(&t1);
	diff=(t1.tv_sec - t0.tv_sec)*1000000 + (t1.tv_usec - t0.tv_usec);
	printk("lat migration exec %ld usec node %d\n",diff,src_cb_index);
#endif
    }//while

    return err;
}


static void client_recv_evict(struct kernel_cb *cb) 
{
    int i;
    // TODO : change this to multiple transactions.
    cb->remote_chunk.client_swapspace_index = cb->recv_buf.size_gb; //client_chunk_index
    cb->remote_chunk.src_cb_index = cb->recv_buf.cb_index; //src_cb_index	
    cb->remote_chunk.src_chunk_index = cb->recv_buf.chunk_index; //src_chunk_index	
    cb->remote_chunk.replica_index = cb->recv_buf.replica_index;

    for (i=0; i<MAX_MR_SIZE_GB; i++){
	if (cb->recv_buf.rkey[i]){
	    cb->remote_chunk.migration_chunk_map[i] = 'm'; // need to migrate
	    printk("rdmabox[%s]: evict mr_info rkey:0x%x addr:0x%x \n",__func__,cb->recv_buf.rkey[i],cb->recv_buf.buf[i]);
	}else{
	    cb->remote_chunk.migration_chunk_map[i] = 0; // not related
	}
    }
}


#if defined(USE_MULTI_SCQ_BUSY_POLL)
static int client_recv(struct ib_wc *wc)
{
    struct kernel_cb *cb = ptr_from_uint64(wc->wr_id);
#else
    static int client_recv(struct kernel_cb *cb, struct ib_wc *wc)
    {
#endif
	struct query_message qmsg;
	struct ib_recv_wr * bad_wr;
	int ret;

	if (wc->byte_len != sizeof(cb->recv_buf)) {
	    printk("rdmabox[%s]: Received bogus data, size %d\n",__func__,wc->byte_len);
	    return -1;
	}	
	if (cb->state < CONNECTED){
	    printk("rdmabox[%s]: cb is not connected\n",__func__);	
	    return -1;
	}
	switch(cb->recv_buf.type){
	    case RESP_QUERY:
		memcpy(&qmsg,&cb->recv_buf,sizeof(qmsg));
		//printk("rdmabox[%s]: RESP_QUERY %d\n", __func__,cb->recv_buf.size_gb);
#ifdef DEBUG
		//printk("rdmabox[%s]: Received RESP_QUERY free_mem : %d, grad : %d, sum : %lu node:%d\n", __func__,qmsg.size_gb, qmsg.grad, qmsg.sum, cb->cb_index);
		printk("rdmabox[%s]: Received RESP_QUERY free_mem : %d, grad : %d, node:%d\n", __func__,qmsg.size_gb, qmsg.grad, cb->cb_index);
#endif
		cb->remote_chunk.target_size_g = qmsg.size_gb;
		//cb->remote_chunk.sum = qmsg.sum;
		cb->remote_chunk.grad = qmsg.grad;
		cb->state = RECV_FREE_MEM;	
		break;
	    case INFO_MIGRATION:
#ifdef DEBUG
		printk("rdmabox[%s]: Received INFO_MIGRATION from node[%d] \n", __func__,cb->cb_index);
#endif
		//cb->RDMABOX_sess->cb_state_list[cb->cb_index] = CB_MIG_MAPPED;
		cb->state = RECV_INFO_MIG;
		RDMABOX_migration_chunk_init(cb);
		break;
	    case INFO_SINGLE:
#ifdef DEBUG
		printk("rdmabox[%s]: Received INFO_SINGLE from node[%d] \n", __func__,cb->cb_index);
#endif
		cb->RDMABOX_sess->cb_state_list[cb->cb_index] = CB_MAPPED;
		cb->state = RECV_INFO_SINGLE;
		RDMABOX_single_chunk_init(cb);
		break;
	    case DONE_MIGRATION:
		cb->state = RECV_MIGRATION_DONE;
		cb->remote_chunk.c_state = C_MIGRATION_DONE;
#ifdef DEBUG
		printk("rdmabox[%s]: Received DONE_MIGRATION from node[%d] \n", __func__,cb->cb_index);
#endif
		wake_up_interruptible(&cb->remote_chunk.sem);
		break;
	    case EVICT:
		cb->state = RECV_EVICT;
		client_recv_evict(cb);
		cb->remote_chunk.c_state = C_EVICT;
#ifdef DEBUG
		printk("rdmabox[%s]: Received EVICT from node[%d] \n", __func__,cb->cb_index);
#endif
		wake_up_interruptible(&cb->remote_chunk.sem);
		break;
		/*
		   case REQ_ACTIVITY:
		   cb->remote_chunk.c_state = C_QUERY;
		   printk("rdmabox[%s]: Received REQ_ACTIVITY from node[%d] \n", __func__,cb->cb_index);
		   wake_up_interruptible(&cb->remote_chunk.sem);
		   break;
		 */
	    default:
#ifdef DEBUG
		printk("rdmabox[%s]: client receives unknown msg %d\n",__func__, cb->recv_buf.type);
#endif
		return -1; 	
	}

	ret = ib_post_recv(cb->qp[0], &cb->rq_wr, &bad_wr);
	if (ret) {
	    printk("rdmabox[%s]: post recv error: %d\n",__func__,ret);
	    return -1;
	}
	if (cb->state == RECV_FREE_MEM || cb->state == RECV_INFO_SINGLE || cb->state == RECV_INFO_MIG){
	    wake_up_interruptible(&cb->sem);
	}

	return 0;
    }

    static int client_send(struct kernel_cb *cb)
    {
	if (cb->state < CONNECTED){
	    printk("rdmabox[%s]: cb is not connected\n",__func__);	
	    return -1;
	}
	switch(cb->send_buf.type){
	    case DELETE_CHUNK:
#ifdef DEBUG
		printk("rdmabox[%s]: DELETE_CHUNK send done \n", __func__);
#endif
		cb->remote_chunk.c_state = C_DELETE_CHUNK;
		wake_up_interruptible(&cb->remote_chunk.sem);
		break;
	    default:
		; 	
	}

	return 0;	
    }

    static int client_read_done(struct kernel_cb * cb, struct ib_wc *wc)
    {
	struct rdma_ctx *ctx;
	struct request *req;
	int i;
	int cbindex;
	int cpuindex;

#ifdef FLOW_CONTROL
	atomic_dec(&num_outstanding);
#endif

	ctx = (struct rdma_ctx *)ptr_from_uint64(wc->wr_id);	
	if (ctx == NULL){
	    printk("rdmabox[%s]: ctx is null\n",__func__);
	    return 1;
	}

	atomic_set(&ctx->in_flight, CTX_IDLE);

	if(ctx->isSGE)
	    dma_unmap_sg(ctx->cb->RDMABOX_sess->pd->device->dma_device, ctx->data_tbl.sgl, ctx->data_tbl.nents, DMA_FROM_DEVICE);

	for(i=0; i<ctx->numreq; i++){
	    req = ctx->reqlist[i];
	    if(!ctx->isSGE){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
		memcpy(bio_data(req->bio), ctx->mr->rdma_buf + (i*RDMABOX_PAGE_SIZE), RDMABOX_PAGE_SIZE);
#else
		memcpy(req->buffer, (ctx->mr->rdma_buf + (i*RDMABOX_PAGE_SIZE)), RDMABOX_PAGE_SIZE);
#endif
	    }

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
	    blk_mq_end_request(req, 0);
#else
	    blk_mq_end_io(req, 0);
#endif
	    ctx->reqlist[i] = NULL;
	}//for

	//printk("rdmabox[%s]: remote read done numreq=%d \n",__func__,ctx->numreq);

	ctx->chunk_index = -1;
	ctx->len=0;
	ctx->numreq=0;
	if(ctx->isSGE){
	    put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->cbhint]->rdma_read_freepool_list[ctx->cpuhint],ctx,NULL);
	}else{
	    if(ctx->cpuhint == ctx->mr->cpuhint && ctx->cbhint == ctx->mr->cbhint){
		put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->cbhint]->rdma_read_freepool_list[ctx->cpuhint],ctx,ctx->mr);
	    }else{
		put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->mr->cbhint]->rdma_read_freepool_list[ctx->mr->cpuhint],NULL,ctx->mr);
		put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->cbhint]->rdma_read_freepool_list[ctx->cpuhint],ctx,NULL);
	    }
	}

	return 0;
    }

static int client_write_done(struct kernel_cb * cb, struct ib_wc *wc)
{
	struct rdma_ctx *ctx=NULL;
	struct local_page_list *sending_item=NULL;
	int i;
	int err;
	int cnt;
	int cbindex;
	int cpuindex;
#ifdef DISKBACKUP
	int num_replica = NUM_REPLICA+1;
#else
	int num_replica = NUM_REPLICA;
#endif

#ifdef FLOW_CONTROL
	atomic_dec(&num_outstanding);
#endif

	ctx = (struct rdma_ctx *)ptr_from_uint64(wc->wr_id);	
	if (ctx == NULL){
	    printk("rdmabox[%s]: ctx is null\n",__func__);
	    return 1;
	}
	if (ctx->chunk_ptr == NULL){
	    printk("rdmabox[%s]: ctx->chunk_ptr is null  \n",__func__);
	    return 1;
	}

	RDMABOX_bitmap_group_set(ctx->chunk_ptr->bitmap_g, ctx->offset, ctx->len);

	if(!ctx->sending_item){
	    cnt = atomic_inc_return(&ctx->prime_ctx->ref_count);
	    //printk("rdmabox[%s]: replica_index:%d cnt:%d \n",__func__,ctx->replica_index,cnt);

	    // prime ctx
	    if(ctx->replica_index == 0){
		//printk("rdmabox[%s]: return primary replica_index:%d \n",__func__,ctx->replica_index);
		if(!ctx->isSGE){
		    for(i=0; i<ctx->numreq; i++){
		        //printk("rdmabox[%s]: replica_index:%d endio reqlist[%d]\n",__func__,ctx->replica_index, i);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
			blk_mq_end_request(ctx->reqlist[i], 0);
#else
			blk_mq_end_io(ctx->reqlist[i], 0);
#endif
		    }//for
		}
		if(cnt < num_replica)
		    return 0;
	    }//prime ctx

	    // last ctx 
	    if(cnt >= num_replica){
		if(ctx->isSGE){
		    //printk("rdmabox[%s]: SGE unmap nents:%d replica_index:%d endio\n",__func__,ctx->data_tbl.nents, ctx->replica_index);
		    dma_unmap_sg(ctx->cb->RDMABOX_sess->pd->device->dma_device, ctx->data_tbl.sgl, ctx->data_tbl.nents, DMA_TO_DEVICE);
		    for(i=0; i<ctx->numreq; i++){
		        //printk("rdmabox[%s]: SGE replica_index:%d endio reqlist[%d]\n",__func__,ctx->replica_index, i);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
			blk_mq_end_request(ctx->reqlist[i], 0);
#else
			blk_mq_end_io(ctx->reqlist[i], 0);
#endif
		    }//for
		}else{
		    put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->prime_ctx->mr->cbhint]->rdma_write_freepool_list[ctx->prime_ctx->mr->cpuhint],NULL,ctx->prime_ctx->mr);
		}

		if(ctx->replica_index != 0){
		    ctx->prime_ctx->sending_item = NULL;
		    ctx->prime_ctx->chunk_index = -1;
		    ctx->prime_ctx->chunk_ptr = NULL;
		    atomic_set(&ctx->prime_ctx->in_flight, CTX_IDLE);
		    put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->prime_ctx->cbhint]->rdma_write_freepool_list[ctx->prime_ctx->cpuhint],ctx->prime_ctx,NULL);
		}
		//printk("rdmabox[%s]: secondary return primary replica_index:%d \n",__func__,ctx->replica_index);
	    }// last ctx

	}else{ // from local mempool
	    sending_item = ctx->sending_item;
	    if (unlikely(!sending_item)){
		printk("rdmabox[%s]: sending item is null  \n",__func__);
		return 1;
	    }
	    cnt = atomic_inc_return(&ctx->sending_item->ref_count);

	    if(ctx->replica_index == 0 && cnt < num_replica )
		return 0;

	    if(cnt >= num_replica){
		for (i=0; i < sending_item->len;i++){
		    struct tree_entry *entry;
		    entry = sending_item->batch_list[i];
		    atomic_dec(&entry->ref_count);
		    if(atomic_read(&entry->ref_count)<=0){
			clear_flag(entry,UPDATING);
			atomic_set(&entry->ref_count,0);
		    }

		    if(!test_flag(entry,RECLAIMABLE)){
			set_flag(entry,RECLAIMABLE);
			err = put_reclaimable_index(entry);
			if(err)
			    printk("rdmabox[%s]: fail to put reclaim list \n",__func__);
		    }
		}//for
		put_free_item(sending_item);
		put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->prime_ctx->mr->cbhint]->rdma_write_freepool_list[ctx->prime_ctx->mr->cpuhint],NULL,ctx->prime_ctx->mr);

		if(ctx->replica_index != 0){
		    ctx->prime_ctx->sending_item = NULL;
		    ctx->prime_ctx->chunk_index = -1;
		    ctx->prime_ctx->chunk_ptr = NULL;
		    atomic_set(&ctx->prime_ctx->in_flight, CTX_IDLE);
		    put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->prime_ctx->mr->cbhint]->rdma_write_freepool_list[ctx->prime_ctx->mr->cpuhint],ctx->prime_ctx,NULL);
		    //printk("rdmabox[%s]: secondary return primary index:%d, replica_index:%d \n",__func__,sending_item->start_index,ctx->replica_index);
		}
	    }
	}// from local mempool

out:
	// free its own ctx
	ctx->sending_item = NULL;
	ctx->chunk_index = -1;
	ctx->chunk_ptr = NULL;
	ctx->numreq = 0;
	atomic_set(&ctx->in_flight, CTX_IDLE);
	put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->cbhint]->rdma_write_freepool_list[ctx->cpuhint],ctx,NULL);
	//printk("rdmabox[%s]: remote write done index:%d, replica_index:%d \n",__func__,sending_item->start_index,ctx->replica_index);

	return 0;
    }

#ifdef USE_EVENT_POLL
    static void rdma_cq_event_handler(struct ib_cq * cq, void *ctx)
    {
	struct kernel_cb *cb=ctx;
	struct ib_wc wc;
	struct ib_recv_wr * bad_wr;
	int ret;

	if (cb->state == ERROR) {
	    printk("rdmabox[%s]: cq completion in ERROR state\n",__func__);
	    return;
	}

	if(cb->cq != cq)
	    panic("cb->cq != cq");

	ib_req_notify_cq(cb->cq, IB_CQ_NEXT_COMP);

	while ((ret = ib_poll_cq(cb->cq, 1, &wc)) == 1) {
	    if (wc.status) {
		if (wc.status == IB_WC_WR_FLUSH_ERR) {
		    printk("rdmabox[%s]: wc.status : IB_WC_WR_FLUSH_ERR\n",__func__);
		    continue;
		} else {
		    printk("rdmabox[%s]: cq completion failed with "
			    "wr_id %Lx status %d %s opcode %d vender_err %x\n",__func__,
			    wc.wr_id, wc.status, wc.opcode, wc.vendor_err);
		    goto error;
		}
	    }	
	    switch (wc.opcode){
		case IB_WC_RECV:
		    ret = client_recv(cb, &wc);
		    if (ret) {
			printk("rdmabox[%s]: recv wc error: %d\n",__func__, ret);
			goto error;
		    }
		    /*
		       ret = ib_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
		       if (ret) {
		       printk("rdmabox[%s]: post recv error: %d\n",__func__,ret);
		       goto error;
		       }
		       if (cb->state == RECV_FREE_MEM || cb->state == RECV_INFO_SINGLE || cb->state == RECV_INFO_MIG){
		       wake_up_interruptible(&cb->sem);
		       }
		     */
		    break;
		case IB_WC_SEND:
		    ret = client_send(cb);
		    if (ret) {
			printk("rdmabox[%s]: send wc error: %d\n",__func__, ret);
			goto error;
		    }
		    break;
		case IB_WC_RDMA_READ:
		    ret = client_read_done(cb, &wc);
		    if (ret) {
			printk("rdmabox[%s]: read wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			goto error;
		    }
		    break;
		case IB_WC_RDMA_WRITE:
		    ret = client_write_done(cb, &wc);
		    if (ret) {
			printk("rdmabox[%s]: write wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			goto error;
		    }
		    break;
		default:
		    printk("rdmabox[%s]: %d Unexpected opcode %d, Shutting down\n", __func__, __LINE__, wc.opcode);
		    goto error;
	    }
	}
	if (ret){
	    printk("rdmabox[%s]: poll error %d\n",__func__, ret);
	    goto error;
	}
	return;
error:
	cb->state = ERROR;
    }
#endif

#ifdef USE_NEW_HYBRID_POLL
    static void rdma_cq_event_handler(struct ib_cq * cq, void *ctx)
    {
	struct kernel_cb *cb=ctx;
	struct ib_wc *wc;
	struct ib_wc wc_arr[MAX_POLL_WC];
	struct ib_recv_wr * bad_wr;
	int ret;
	int i;
	int num_wc=0;
	int max=0;

	if (cb->state == ERROR) {
	    printk("rdmabox[%s]: cq completion in ERROR state\n",__func__);
	    return;
	}

	if(cb->cq != cq)
	    panic("cb->cq != cq");

	while(1){

	    for(i=0; i<MAX_POLL_WC; i++){
		ret = ib_poll_cq(cb->cq, 1, &wc_arr[i]);
		if (unlikely(ret <= 0))
		    break;
		num_wc++;
	    }

	    for(i=0; i<num_wc; i++){
		wc = &wc_arr[i];
		if (wc->status) {
		    if (wc->status == IB_WC_WR_FLUSH_ERR) {
			printk("rdmabox[%s]: wc.status : IB_WC_WR_FLUSH_ERR\n",__func__);
			continue;
		    } else {
			printk("rdmabox[%s]: cq completion failed with "
				"wr_id %Lx status %d %s opcode %d vender_err %x\n",__func__,
				wc->wr_id, wc->status, wc->opcode, wc->vendor_err);
			continue;
		    }
		}
		switch (wc->opcode){
		    case IB_WC_RECV:
			ret = client_recv(cb, wc);
			if (ret) {
			    printk("rdmabox[%s]: recv wc error: %d\n",__func__, ret);
			    continue;
			}
			break;
		    case IB_WC_SEND:
			ret = client_send(cb);
			if (ret) {
			    printk("rdmabox[%s]: send wc error: %d\n",__func__, ret);
			    continue;
			}
			break;
		    case IB_WC_RDMA_READ:
			ret = client_read_done(cb, wc);
			if (ret) {
			    printk("rdmabox[%s]: read wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			    continue;
			}
			break;
		    case IB_WC_RDMA_WRITE:
			ret = client_write_done(cb, wc);
			if (ret) {
			    printk("rdmabox[%s]: write wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			    continue;
			}
			break;

		    default:
			printk("rdmabox[%s]: %d Unexpected opcode %d, Shutting down\n", __func__, __LINE__, wc->opcode);
			continue;
		}
	    }// for

	    if(num_wc == 0){
		if(max >= RETRY){
		    ib_req_notify_cq(cb->cq, IB_CQ_NEXT_COMP);
		    return;
		}
		max++;
	    }/*else{
	       max=0;
	       num_wc=0;
	       }
	      */
	    num_wc=0;
	}//while

	if (ret){
	    printk("rdmabox[%s]: poll error %d\n",__func__, ret);
	    goto error;
	}

	return;

error:
	cb->state = ERROR;
    }
#endif

#ifdef USE_EVENT_BATCH_POLL
    static void rdma_cq_event_handler(struct ib_cq * cq, void *ctx)
    {
	struct kernel_cb *cb=ctx;
	struct ib_wc *wc;
	struct ib_wc wc_arr[MAX_POLL_WC];
	struct ib_recv_wr * bad_wr;
	int ret=0;
	int num_wc=0;
	int i;
	int budget = MAX_POLL_WC;

	if (cb->state == ERROR) {
	    printk("rdmabox[%s]: cq completion in ERROR state\n",__func__);
	    return;
	}

	if(cb->cq != cq)
	    panic("cb->cq != cq");

	while(budget){

	    for(i=0; i<MAX_POLL_WC; i++){
		ret = ib_poll_cq(cb->cq, 1, &wc_arr[i]);
		if (unlikely(ret <= 0))
		    break;
		num_wc++;
	    }
	    budget -= num_wc;

	    for(i=0; i<num_wc; i++){
		wc = &wc_arr[i];
		if (wc->status) {
		    if (wc->status == IB_WC_WR_FLUSH_ERR) {
			printk("rdmabox[%s]: wc.status : IB_WC_WR_FLUSH_ERR\n",__func__);
			continue;
		    } else {
			printk("rdmabox[%s]: cq completion failed with "
				"wr_id %Lx status %d %s opcode %d vender_err %x\n",__func__,
				wc->wr_id, wc->status, wc->opcode, wc->vendor_err);
			continue;
		    }
		}
		switch (wc->opcode){
		    case IB_WC_RECV:
			ret = client_recv(cb, wc);
			if (ret) {
			    printk("rdmabox[%s]: recv wc error: %d\n",__func__, ret);
			    continue;
			}
			break;
		    case IB_WC_SEND:
			ret = client_send(cb);
			if (ret) {
			    printk("rdmabox[%s]: send wc error: %d\n",__func__, ret);
			    continue;
			}
			break;
		    case IB_WC_RDMA_READ:
			ret = client_read_done(cb, wc);
			if (ret) {
			    printk("rdmabox[%s]: read wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			    continue;
			}
			break;
		    case IB_WC_RDMA_WRITE:
			ret = client_write_done(cb, wc);
			if (ret) {
			    printk("rdmabox[%s]: write wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			    continue;
			}
			break;

		    default:
			printk("rdmabox[%s]: %d Unexpected opcode %d, Shutting down\n", __func__, __LINE__, wc->opcode);
			continue;
		}
	    }// for

	    if(num_wc != MAX_POLL_WC)
		break;

	    num_wc=0;
	}//while

	if (ret){
	    printk("rdmabox[%s]: poll error %d\n",__func__, ret);
	    goto error;
	}

	ib_req_notify_cq(cb->cq, IB_CQ_NEXT_COMP);

	return;
error:
	cb->state = ERROR;
    }
#endif

#if defined(USE_BUSY_POLL) || defined(USE_MULTI_SCQ_BUSY_POLL)
    static void rdma_cq_event_handler(struct ib_cq * cq, void *ctx)
    {
	// do nothing
    }
#endif

#ifdef USE_BUSY_POLL
    void wc_handler(void * data)
    {
	struct kernel_cb * cb = data;
	struct ib_cq * cq = cb->cq;
	struct ib_wc wc;
	struct ib_recv_wr * bad_wr;
	int err;
	int ret;
	int ne;

	while(1){
	    //printk("[%s]stay in busy mode scq:%p\n",__func__,cq);
	    do{
		ne = ib_poll_cq(cq, 1, &wc);
		if(ne==0){
		    schedule();
		}
	    }while(ne <= 0);

	    if(ne < 0){
		printk(KERN_ALERT "poll CQ failed %d\n", ne);
		continue;
	    }

	    if (wc.status) {
		if (wc.status == IB_WC_WR_FLUSH_ERR) {
		    printk("rdmabox[%s]: wc.status : IB_WC_WR_FLUSH_ERR\n",__func__);
		    continue;
		} else {
		    printk("rdmabox[%s]: cq completion failed with "
			    "wr_id %Lx status %d %s opcode %d vender_err %x\n",__func__,
			    wc.wr_id, wc.status, wc.opcode, wc.vendor_err);
		    goto error;
		}
	    }	
	    switch (wc.opcode){
		case IB_WC_RECV:
		    ret = client_recv(cb, &wc);
		    if (ret) {
			printk("rdmabox[%s]: recv wc error: %d\n",__func__, ret);
			goto error;
		    }
		    /*
		       ret = ib_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
		       if (ret) {
		       printk("rdmabox[%s]: post recv error: %d\n",__func__,ret);
		       goto error;
		       }
		       if (cb->state == RECV_FREE_MEM || cb->state == RECV_INFO_SINGLE || cb->state == RECV_INFO_MIG){
		       wake_up_interruptible(&cb->sem);
		       }
		     */
		    break;
		case IB_WC_SEND:
		    ret = client_send(cb);
		    if (ret) {
			printk("rdmabox[%s]: send wc error: %d\n",__func__, ret);
			goto error;
		    }
		    break;
		case IB_WC_RDMA_READ:
		    ret = client_read_done(cb, &wc);
		    if (ret) {
			printk("rdmabox[%s]: read wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			goto error;
		    }
		    break;
		case IB_WC_RDMA_WRITE:
		    ret = client_write_done(cb, &wc);
		    if (ret) {
			printk("rdmabox[%s]: write wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			goto error;
		    }
		    break;
		default:
		    printk("rdmabox[%s]: %d Unexpected opcode %d, Shutting down\n", __func__, __LINE__, wc.opcode);
		    goto error;
	    }
	}

	if (ret){
	    printk("rdmabox[%s]: poll error %d\n",__func__, ret);
	    goto error;
	}
	return;
error:
	cb->state = ERROR;
    }
#endif

#if defined(USE_MULTI_SCQ_BUSY_POLL)
    void wc_handler(void * data)
    {
	struct cq_thread_data *tdata = data;
	struct RDMABOX_session *RDMABOX_sess = tdata->RDMABOX_sess;
	struct ib_cq *cq = tdata->scq;

	struct rdma_ctx * ctx=NULL;
	struct kernel_cb *cb;
	struct ib_recv_wr * bad_wr;
	int err;
	int ret;
	int ne;

	//printk("[%s]start busy polling scq:%p\n",__func__,cq);

	while(1){
	    struct ib_wc wc;
	    //printk("[%s]stay in busy mode scq:%p\n",__func__,cq);
	    do{
		ne = ib_poll_cq(cq, 1, &wc);
		if(ne==0){
		    schedule();
		}
	    }while(ne <= 0);

	    if(ne < 0){
		printk(KERN_ALERT "poll CQ failed %d\n", ne);
		continue;
	    }
	    //printk("[%s]hit, ne %d scq %p wc.wr_id %p\n",__func__,ne,cq,wc.wr_id);

	    // call handler
	    if (wc.status) {
		if (wc.status == IB_WC_WR_FLUSH_ERR) {
		    printk("rdmabox[%s]: wc.status : IB_WC_WR_FLUSH_ERR\n",__func__);
		    continue;
		} else {
		    printk("rdmabox[%s]: cq completion failed with "
			    "wr_id %Lx status %d %s opcode %d vender_err %x\n",__func__,
			    wc.wr_id, wc.status, wc.opcode, wc.vendor_err);
		    goto error;
		}
	    }
	    switch (wc.opcode){
		case IB_WC_RECV:
		    //printk("rdmabox[%s]: IB_WC_RECV\n",__func__);
		    ret = client_recv(&wc);
		    if (ret) {
			printk("rdmabox[%s]: recv wc error: %d\n",__func__, ret);
			goto error;
		    }
		    break;
		case IB_WC_SEND:
		    //printk("rdmabox[%s]: IB_WC_SEND\n",__func__);
		    cb = ptr_from_uint64(wc.wr_id);
		    ret = client_send(cb);
		    if (ret) {
			printk("rdmabox[%s]: send wc error: %d\n",__func__, ret);
			goto error;
		    }
		    break;
		case IB_WC_RDMA_READ:
		    //printk("rdmabox[%s]: IB_WC_RDMA_READ\n",__func__);
		    ctx = (struct rdma_ctx *)ptr_from_uint64(wc.wr_id);
		    cb = ctx->cb;
		    ret = client_read_done(cb, &wc);
		    if (ret) {
			printk("rdmabox[%s]: read wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			goto error;
		    }
		    break;
		case IB_WC_RDMA_WRITE:
		    //printk("rdmabox[%s]: IB_WC_RDMA_WRITE\n",__func__);
		    ctx = (struct rdma_ctx *)ptr_from_uint64(wc.wr_id);
		    cb = ctx->cb;
		    ret = client_write_done(cb, &wc);
		    if (ret) {
			printk("rdmabox[%s]: write wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
			goto error;
		    }
		    break;
		default:
		    printk("rdmabox[%s]: %d Unexpected opcode %d, Shutting down\n", __func__, __LINE__, wc.opcode);
		    goto error;
	    }//switch

	}//while

error:
	cb->state = ERROR;
    }
#endif



static void RDMABOX_setup_wr(struct kernel_cb *cb)
{

#ifdef DEBUG
	printk("rdmabox[%s]: \n", __func__);

#endif
	cb->recv_sgl.addr = cb->recv_dma_addr;
	cb->recv_sgl.length = sizeof cb->recv_buf;
	if (cb->local_dma_lkey)
	    cb->recv_sgl.lkey = cb->qp[0]->device->local_dma_lkey;
	else if (cb->mem == DMA)
	    cb->recv_sgl.lkey = cb->dma_mr->lkey;
	cb->rq_wr.sg_list = &cb->recv_sgl;
	cb->rq_wr.num_sge = 1;

#if defined(USE_MULTI_SCQ_BUSY_POLL)
	cb->rq_wr.wr_id = uint64_from_ptr(cb);
	//printk("rdmabox[%s]: receive queue wr_id:%p\n",__func__,cb->rq_wr.wr_id);
#endif

	cb->send_sgl.addr = cb->send_dma_addr;
	cb->send_sgl.length = sizeof cb->send_buf;
	if (cb->local_dma_lkey)
	    cb->send_sgl.lkey = cb->qp[0]->device->local_dma_lkey;
	else if (cb->mem == DMA)
	    cb->send_sgl.lkey = cb->dma_mr->lkey;
	cb->sq_wr.opcode = IB_WR_SEND;
	cb->sq_wr.send_flags = IB_SEND_SIGNALED;
	cb->sq_wr.sg_list = &cb->send_sgl;
	cb->sq_wr.num_sge = 1;
#if defined(USE_MULTI_SCQ_BUSY_POLL)
	cb->sq_wr.wr_id = uint64_from_ptr(cb);
	//printk("rdmabox[%s]: send queue wr_id:%p\n",__func__,cb->sq_wr.wr_id);
#endif
    }

    static int RDMABOX_setup_buffers(struct kernel_cb *cb, struct RDMABOX_session *RDMABOX_session)
    {
	int ret;

#ifdef DEBUG
	printk("rdmabox[%s]: RDMABOX_setup_buffers called on cb=%p size cb->recv_buf=%lu\n",__func__, cb,sizeof(cb->recv_buf));

#endif
/*
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 11, 0)	
	cb->recv_dma_addr = dma_map_single(&RDMABOX_session->pd->device->dev, 
		&cb->recv_buf, sizeof(cb->recv_buf), DMA_BIDIRECTIONAL);
#else
	*/
	cb->recv_dma_addr = dma_map_single(RDMABOX_session->pd->device->dma_device, 
		&cb->recv_buf, sizeof(cb->recv_buf), DMA_BIDIRECTIONAL);
//#endif
	pci_unmap_addr_set(cb, recv_mapping, cb->recv_dma_addr);
	/*
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 11, 0)
	cb->send_dma_addr = dma_map_single(&RDMABOX_session->pd->device->dev, 
		&cb->send_buf, sizeof(cb->send_buf), DMA_BIDIRECTIONAL);	
#else
	*/
	cb->send_dma_addr = dma_map_single(RDMABOX_session->pd->device->dma_device, 
		&cb->send_buf, sizeof(cb->send_buf), DMA_BIDIRECTIONAL);
//#endif
	pci_unmap_addr_set(cb, send_mapping, cb->send_dma_addr);
#ifdef DEBUG
	printk("rdmabox[%s]: cb->mem=%d \n",__func__, cb->mem);

#endif

	if (cb->mem == DMA) {
#ifdef DEBUG
	    printk("rdmabox[%s]: RDMABOX_setup_buffers, in cb->mem==DMA \n",__func__);

#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 11, 0)
	    cb->dma_mr = RDMABOX_session->pd->device->get_dma_mr(RDMABOX_session->pd, IB_ACCESS_LOCAL_WRITE|
		    IB_ACCESS_REMOTE_READ|
		    IB_ACCESS_REMOTE_WRITE);
#else
	    cb->dma_mr = ib_get_dma_mr(RDMABOX_session->pd, IB_ACCESS_LOCAL_WRITE|
		    IB_ACCESS_REMOTE_READ|
		    IB_ACCESS_REMOTE_WRITE);
#endif
	    if (IS_ERR(cb->dma_mr)) {
		printk("rdmabox[%s]: reg_dmamr failed\n",__func__);

		ret = PTR_ERR(cb->dma_mr);
		goto bail;
	    }
	} 

	RDMABOX_setup_wr(cb);
#ifdef DEBUG
	printk("rdmabox[%s]: allocated & registered buffers...\n",__func__);

#endif
	return 0;
bail:

	if (cb->dma_mr && !IS_ERR(cb->dma_mr))
	    ib_dereg_mr(cb->dma_mr);
	if (cb->recv_mr && !IS_ERR(cb->recv_mr))
	    ib_dereg_mr(cb->recv_mr);
	if (cb->send_mr && !IS_ERR(cb->send_mr))
	    ib_dereg_mr(cb->send_mr);

	return ret;
    }

    static void RDMABOX_free_buffers(struct kernel_cb *cb)
    {
#ifdef DEBUG
	printk("rdmabox[%s]: RDMABOX_free_buffers called on cb %p\n",__func__, cb);

#endif	
	if (cb->dma_mr)
	    ib_dereg_mr(cb->dma_mr);
	if (cb->send_mr)
	    ib_dereg_mr(cb->send_mr);
	if (cb->recv_mr)
	    ib_dereg_mr(cb->recv_mr);

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 11, 0)	
	dma_unmap_single(&cb->RDMABOX_sess->pd->device->dev,
		pci_unmap_addr(cb, recv_mapping),
		sizeof(cb->recv_buf), DMA_BIDIRECTIONAL);
	dma_unmap_single(&cb->RDMABOX_sess->pd->device->dev,
		pci_unmap_addr(cb, send_mapping),
		sizeof(cb->send_buf), DMA_BIDIRECTIONAL);
#else
	dma_unmap_single(cb->RDMABOX_sess->pd->device->dma_device,
		pci_unmap_addr(cb, recv_mapping),
		sizeof(cb->recv_buf), DMA_BIDIRECTIONAL);
	dma_unmap_single(cb->RDMABOX_sess->pd->device->dma_device,
		pci_unmap_addr(cb, send_mapping),
		sizeof(cb->send_buf), DMA_BIDIRECTIONAL);
#endif
    }

static int RDMABOX_create_qp(struct kernel_cb *cb, struct rdma_cm_id *cm_id, struct RDMABOX_session *RDMABOX_session, int index)
{
	//struct ib_device_attr device_attr;
	struct ib_qp_init_attr init_attr;
	int ret;

	if(!cm_id->device)
	    printk("rdmabox[%s]: cm_id->device is  null \n", __func__);

        // kernel 4.15 doesn't have this
	//ib_query_device(cm_id->device,&device_attr);
	//printk("rdmabox[%s]: max_sge:%d max_qp_wr:%d \n", __func__,device_attr.max_sge,device_attr.max_qp_wr);

	memset(&init_attr, 0, sizeof(init_attr));
	//init_attr.cap.max_send_wr = device_attr.max_qp_wr;
	init_attr.cap.max_send_wr = RDMABOX_QUEUE_DEPTH * submit_queues + 1;;
	//init_attr.cap.max_recv_wr = device_attr.max_qp_wr;  
	init_attr.cap.max_recv_wr = RDMABOX_QUEUE_DEPTH * submit_queues + 1;  
	//init_attr.cap.max_recv_sge = device_attr.max_sge;
	//init_attr.cap.max_send_sge = device_attr.max_sge;
	init_attr.cap.max_recv_sge = MAX_SGE;
	init_attr.cap.max_send_sge = MAX_SGE;
	init_attr.qp_type = IB_QPT_RC;
	init_attr.send_cq = cb->cq;
	init_attr.recv_cq = cb->cq;
	init_attr.sq_sig_type = IB_SIGNAL_REQ_WR;

	ret = rdma_create_qp(cm_id, RDMABOX_session->pd, &init_attr);
	if (!ret)
	    cb->qp[index] = cm_id->qp;
	return ret;
    }

    static void RDMABOX_free_qp(struct kernel_cb *cb)
    {
	ib_destroy_qp(cb->qp[0]);
	ib_destroy_cq(cb->cq);
	//ib_dealloc_pd(cb->pd);
    }

    static int RDMABOX_setup_cq(struct kernel_cb *cb, struct rdma_cm_id *cm_id, struct RDMABOX_session *RDMABOX_session)
    {
	int tid;
	int sid;
	struct task_struct *ts;
	//struct ib_device_attr device_attr;
	int ret;
	int i;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	struct ib_cq_init_attr init_attr;
#endif

        // kernel 4.15 doesn't have this
	//ib_query_device(cm_id->device,&device_attr);
	//printk("rdmabox[%s]: max_cqe:%d \n", __func__,device_attr.max_cqe);

#if defined(USE_EVENT_POLL) || defined(USE_EVENT_BATCH_POLL) || defined(USE_NEW_HYBRID_POLL)
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.cqe = cb->txdepth * 2;
	//init_attr.cqe = device_attr.max_cqe;
	init_attr.comp_vector = 0;

	cb->cq = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, cb, &init_attr);
#else
	cb->cq = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, cb, cb->txdepth * 2, 0);
	//cb->cq = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, cb, device_attr.max_cqe, 0);
#endif
#endif // EVENT_POLL

#ifdef USE_BUSY_POLL
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.cqe = cb->txdepth * 2;
	//init_attr.cqe = device_attr.max_cqe;
	init_attr.comp_vector = 0;
	cb->cq = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, cb, &init_attr);
#else// KERNEL_VERSION
	cb->cq = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, cb, cb->txdepth * 2, 0);
	//cb->cq = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, cb, device_attr.max_cqe, 0);
#endif
	char thread_name[10]="cqpoll";
	printk("rdmabox[%s]: create poll thread %s \n",__func__,thread_name);
	ts = kthread_create(wc_handler, cb, thread_name);
	wake_up_process(ts);
#endif //USE_BUSY_POLL

#if defined(USE_MULTI_SCQ_BUSY_POLL)
	sid = atomic_read(&RDMABOX_session->curr_scq_index);
	if(atomic_inc_return(&RDMABOX_session->curr_scq_index) >= NUM_SCQ)
	    atomic_set(&RDMABOX_session->curr_scq_index,0);

	struct cq_thread_data *tdata;
	tdata = &RDMABOX_session->tdata[sid];
	tdata->RDMABOX_sess = RDMABOX_session;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.cqe = cb->txdepth * 2;
	//init_attr.cqe = device_attr.max_cqe;
	init_attr.comp_vector = 0;
	if(!RDMABOX_session->scq[sid]){
	    RDMABOX_session->scq[sid] = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, tdata, &init_attr);
	    printk(" rdmabox[%s]: created scq[%d] %p\n",__func__,sid, RDMABOX_session->scq[sid]);
#else// KERNEL_VERSION
	    if(!RDMABOX_session->scq[sid]){
		RDMABOX_session->scq[sid] = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, tdata, cb->txdepth * 2, 0);
		//RDMABOX_session->scq[sid] = ib_create_cq(cm_id->device, rdma_cq_event_handler, NULL, tdata, device_attr.max_cqe, 0);
		printk(" rdmabox[%s]: created scq[%d] %p\n",__func__,sid, RDMABOX_session->scq[sid]);
#endif

		tdata->scq = RDMABOX_session->scq[sid];
		for(i=0;i<NUM_POLL_THREAD;i++){
		    tid = atomic_read(&RDMABOX_session->curr_thread_index);
		    if(atomic_inc_return(&RDMABOX_session->curr_thread_index) >= (NUM_POLL_THREAD * NUM_SCQ))
			atomic_set(&RDMABOX_session->curr_thread_index,0);
		    char thread_name[16]={};
		    sprintf(thread_name, "cqpoll%d",tid);
		    printk("rdmabox[%s]: create poll thread %s with scq:%p \n",__func__,thread_name,tdata->scq);
		    ts = kthread_create(wc_handler, tdata, thread_name);
		    wake_up_process(ts);
		}
	    }// !RDMABOX_session->scq[sid]

	    cb->cq = RDMABOX_session->scq[sid];
	    printk("rdmabox[%s]: assign scq[%d]:%p to cb:%p \n",__func__,sid,cb->cq,cb);
#endif //USE_MULTI_SCQ_BUSY_POLL

	    if (IS_ERR(cb->cq)) {
		printk("rdmabox[%s]: ib_create_cq failed\n",__func__);

		ret = PTR_ERR(cb->cq);
		goto err1;
	    }
#ifdef DEBUG
	    printk("rdmabox[%s]: created cq %p\n",__func__, cb->cq);
#endif

	    ret = ib_req_notify_cq(cb->cq, IB_CQ_NEXT_COMP);
	    if (ret) {
		printk("rdmabox[%s]: ib_create_cq failed\n",__func__);

		goto err2;
	    }

	    return 0;
err2:
	    ib_destroy_cq(cb->cq);
err1:
	    //ib_dealloc_pd(cb->pd);
	    return ret;
	}

	static void fill_sockaddr(struct sockaddr_storage *sin, struct kernel_cb *cb)
	{
#ifdef DEBUG
	    printk("rdmabox[%s]: \n", __func__);

#endif
	    memset(sin, 0, sizeof(*sin));

	    if (cb->addr_type == AF_INET) {
		struct sockaddr_in *sin4 = (struct sockaddr_in *)sin;
		sin4->sin_family = AF_INET;
		memcpy((void *)&sin4->sin_addr.s_addr, cb->addr, 4);
		sin4->sin_port = cb->port;
	    } else if (cb->addr_type == AF_INET6) {
		struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)sin;
		sin6->sin6_family = AF_INET6;
		memcpy((void *)&sin6->sin6_addr, cb->addr, 16);
		sin6->sin6_port = cb->port;
	    }
	}

	static int RDMABOX_connect_client(struct kernel_cb *cb, struct rdma_cm_id *cm_id)
	{
	    struct rdma_conn_param conn_param;
	    struct conn_private_data pdata;
	    struct RDMABOX_session *RDMABOX_sess = cb->RDMABOX_sess;
	    int ret;
	    //int tmp_cb_index = -1;
	    pdata.mig_cb_index = -1;
#ifdef DEBUG
	    printk("rdmabox[%s]: \n",__func__);
#endif

	    memcpy(pdata.addr, RDMABOX_sess->node_addr, sizeof(RDMABOX_sess->node_addr));

	    memset(&conn_param, 0, sizeof conn_param);
	    conn_param.responder_resources = 1;
	    conn_param.initiator_depth = 1;
	    conn_param.retry_count = 7;
	    //conn_param.private_data = (int*)&tmp_cb_index; // mark for client cb connection
	    //conn_param.private_data_len = sizeof(int);
	    conn_param.private_data = &pdata;
	    conn_param.private_data_len = sizeof(struct conn_private_data);

	    ret = rdma_connect(cm_id, &conn_param);
	    if (ret) {
		printk("rdmabox[%s]: rdma_connect error %d\n",__func__, ret);

		return ret;
	    }

	    wait_event_interruptible(cb->sem, cb->state >= CONNECTED);
	    if (cb->state == ERROR) {
		printk("rdmabox[%s]: wait for CONNECTED state %d\n",__func__, cb->state);

		return -1;
	    }

#ifdef DEBUG
	    printk("rdmabox[%s]: rdma_connect successful\n",__func__);

#endif
	    return 0;
	}

	static int RDMABOX_bind_client(struct kernel_cb *cb, struct rdma_cm_id *cm_id)
	{
	    struct sockaddr_storage sin;
	    int ret;

#ifdef DEBUG
	    printk("rdmabox[%s]: \n", __func__);

#endif
	    fill_sockaddr(&sin, cb);

	    ret = rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&sin, 2000);
	    if (ret) {
		printk("rdmabox[%s]: rdma_resolve_addr error %d\n",__func__, ret);

		return ret;
	    }

	    wait_event_interruptible(cb->sem, cb->state >= ROUTE_RESOLVED);
	    if (cb->state != ROUTE_RESOLVED) {
		printk("rdmabox[%s]: addr/route resolution did not resolve: state %d\n",__func__,cb->state);

		return -EINTR;
	    }
#ifdef DEBUG
	    printk("rdmabox[%s]: rdma_resolve_addr - rdma_resolve_route successful\n",__func__);

#endif
	    return 0;
	}

	const char *RDMABOX_device_state_str(struct RDMABOX_file *dev)
	{
	    char *state;

	    spin_lock(&dev->state_lock);
	    switch (dev->state) {
		case DEVICE_INITIALIZING:
		    state = "Initial state";
		    break;
		case DEVICE_OPENNING:
		    state = "openning";
		    break;
		case DEVICE_RUNNING:
		    state = "running";
		    break;
		case DEVICE_OFFLINE:
		    state = "offline";
		    break;
		default:
		    state = "unknown device state";
	    }
	    spin_unlock(&dev->state_lock);

	    return state;
	}

	// TODO : should it be in rpg_drv.c ?
	int RDMABOX_create_device(struct RDMABOX_session *RDMABOX_session,
		const char *xdev_name, struct RDMABOX_file *RDMABOX_file)
	{
	    int retval;
	    // char name[20];
	    sscanf(xdev_name, "%s", RDMABOX_file->file_name);
	    RDMABOX_file->index = RDMABOX_indexes++;
	    RDMABOX_file->nr_queues = submit_queues;
	    RDMABOX_file->queue_depth = RDMABOX_QUEUE_DEPTH;
	    RDMABOX_file->RDMABOX_sess = RDMABOX_session;
	    //RDMABOX_file->RDMABOX_conns = RDMABOX_session->RDMABOX_conns;
	    printk("rdmabox[%s]: In RDMABOX_create_device(), dev_name:%s\n",__func__, xdev_name);

	    retval = RDMABOX_setup_queues(RDMABOX_file); // prepare enough queue items for each working threads
	    if (retval) {
		printk("rdmabox[%s]: RDMABOX_setup_queues failed\n", __func__);

		goto err;
	    }
	    RDMABOX_file->stbuf.st_size = RDMABOX_session->capacity;
	    printk("rdmabox[%s]: st_size = %lu\n",__func__, RDMABOX_file->stbuf.st_size);


	    RDMABOX_session->xdev = RDMABOX_file;
	    retval = RDMABOX_register_block_device(RDMABOX_file);
	    if (retval) {
		printk("rdmabox[%s]: failed to register IS device %s ret=%d\n",__func__,
			RDMABOX_file->file_name, retval);

		goto err_queues;
	    }

	    msleep(2000);
	    RDMABOX_set_device_state(RDMABOX_file, DEVICE_RUNNING);
	    return 0;

err_queues:
	    RDMABOX_destroy_queues(RDMABOX_file);
err:
	    return retval;
	}

	void RDMABOX_destroy_device(struct RDMABOX_session *RDMABOX_session,
		struct RDMABOX_file *RDMABOX_file)
	{
	    printk("rdmabox[%s]: \n", __func__);


	    RDMABOX_set_device_state(RDMABOX_file, DEVICE_OFFLINE);
	    if (RDMABOX_file->disk){
		RDMABOX_unregister_block_device(RDMABOX_file);  
		RDMABOX_destroy_queues(RDMABOX_file);  
	    }

	    spin_lock(&RDMABOX_session->devs_lock);
	    list_del(&RDMABOX_file->list);
	    spin_unlock(&RDMABOX_session->devs_lock);
	}

	static void RDMABOX_destroy_session_devices(struct RDMABOX_session *RDMABOX_session)
	{
	    struct RDMABOX_file *xdev, *tmp;
	    printk("rdmabox[%s]: \n",__func__);

	    list_for_each_entry_safe(xdev, tmp, &RDMABOX_session->devs_list, list) {
		RDMABOX_destroy_device(RDMABOX_session, xdev);
	    }
	}
	/*
	   static void RDMABOX_destroy_conn(struct RDMABOX_connection *RDMABOX_conn)
	   {
	   RDMABOX_conn->RDMABOX_sess = NULL;
	   RDMABOX_conn->conn_th = NULL;
	   printk("rdmabox[%s]: \n", __func__);


	   kfree(RDMABOX_conn);
	   }

	   static int RDMABOX_create_conn(struct RDMABOX_session *RDMABOX_session, int cpu,
	   struct RDMABOX_connection **conn)
	   {
	   struct RDMABOX_connection *RDMABOX_conn;
	   int ret = 0;
	   int i;	

	   RDMABOX_conn = kzalloc(sizeof(*RDMABOX_conn), GFP_KERNEL);
	   if (!RDMABOX_conn) {
	   printk("rdmabox[%s]: failed to allocate RDMABOX_conn",__func__);

	   return -ENOMEM;
	   }
	   RDMABOX_conn->RDMABOX_sess = RDMABOX_session;
	   RDMABOX_conn->cpu_id = cpu;

	 *conn = RDMABOX_conn;

	 return ret;
	 }
	 */
	static int rdma_connect_down(struct kernel_cb *cb, struct ib_qp *qp, struct rdma_cm_id *cm_id)
	{
	    struct ib_recv_wr *bad_wr;
	    int ret;

	    //#ifdef DEBUG
	    //  printk("rdmabox[%s]: \n", __func__);
	    //#endif
	    ret = ib_post_recv(qp, &cb->rq_wr, &bad_wr); 
	    if (ret) {
		printk("rdmabox[%s]: ib_post_recv failed: %d\n",__func__, ret);

		goto err;
	    }

	    ret = RDMABOX_connect_client(cb, cm_id);  
	    if (ret) {
		printk("rdmabox[%s]: connect error %d\n",__func__, ret);
		goto err;
	    }

	    return 0;

err:
	    RDMABOX_free_buffers(cb);
	    return ret;
	}

	static int rdma_connect_upper(struct kernel_cb *cb, struct RDMABOX_session *RDMABOX_session)
	{
	    int ret;
	    int i;
	    struct rdma_cm_id *cm_id;

	    for(i=0; i< NUM_CHANNEL; i++){
		cm_id = cb->cm_id[i];
		printk(" rdmabox[%s]: setup for cm_id %p\n",__func__, cm_id);
		cb->state = IDLE;     
		ret = RDMABOX_bind_client(cb, cm_id);
		if (ret)
		    return ret;

		if(!RDMABOX_session->pd){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 11, 0)
		    RDMABOX_session->pd = ib_alloc_pd(cm_id->device, IB_ACCESS_LOCAL_WRITE|
			    IB_ACCESS_REMOTE_READ|
			    IB_ACCESS_REMOTE_WRITE );
#else
		    RDMABOX_session->pd = ib_alloc_pd(cm_id->device);
#endif
		    if (IS_ERR(RDMABOX_session->pd)) {
			printk("rdmabox[%s]: ib_alloc_pd failed\n",__func__);
			return PTR_ERR(RDMABOX_session->pd);
		    }
		    //#ifdef DEBUG
		    printk(" rdmabox[%s]: created pd %p\n",__func__, RDMABOX_session->pd);
		    //#endif
		}

		if(!cb->cq){
		    ret = RDMABOX_setup_cq(cb, cm_id, RDMABOX_session);
		    if (ret) {
			printk("rdmabox[%s]: setup_qp failed: %d\n",__func__, ret);
			return ret;
		    }
		}

		ret = RDMABOX_create_qp(cb, cm_id, RDMABOX_session, i);
		if (ret) {
		    printk("rdmabox[%s]: create_qp failed: %d\n",__func__, ret);
		    return ret;
		}
		printk("rdmabox[%s]: create qp %p\n",__func__, cb->qp[i]);
	    }//for

	    ret = RDMABOX_setup_buffers(cb, RDMABOX_session);
	    if (ret) {
		printk("rdmabox[%s]: RDMABOX_setup_buffers failed: %d\n",__func__, ret);
		goto err1;
	    }

	    return 0;

err1:
	    RDMABOX_free_qp(cb);	

	    return ret;
	}

	static void portal_parser(struct RDMABOX_session *RDMABOX_session)
	{
	    char *ptr = RDMABOX_session->portal + 7;	//rdma://[]
	    char *node_portal = NULL;
	    char *single_portal = NULL;
	    int p_count=0, i=0, j=0;
	    int port = 0;
	    /*
	       server_portal = strsep(&ptr, ",");
	       j = 0;
	       while (*(server_portal + j) != ','){
	       j++;
	       }
	       memcpy(RDMABOX_session->node_addr, node_portal, j);
	       RDMABOX_session->node_addr[j] = '\0';
	     */
	    sscanf(strsep(&ptr, ","), "%s", RDMABOX_session->node_addr);
	    printk("rdmabox[%s]: local node portal: %s\n",__func__, RDMABOX_session->node_addr);

	    sscanf(strsep(&ptr, ","), "%d", &p_count);
	    NUM_CB = p_count; // server count
	    RDMABOX_session->cb_num = NUM_CB;
	    RDMABOX_session->portal_list = kzalloc(sizeof(struct RDMABOX_portal) * RDMABOX_session->cb_num, GFP_KERNEL);	

	    for (i=0; i < p_count; i++){
		single_portal = strsep(&ptr, ",");

		j = 0;
		while (*(single_portal + j) != ':'){
		    j++;
		}
		memcpy(RDMABOX_session->portal_list[i].addr, single_portal, j);
		RDMABOX_session->portal_list[i].addr[j] = '\0';
		port = 0;
		sscanf(single_portal+j+1, "%d", &port);
		RDMABOX_session->portal_list[i].port = (uint16_t)port; 
		printk("rdmabox[%s]: portal: %s, %d\n",__func__, RDMABOX_session->portal_list[i].addr, RDMABOX_session->portal_list[i].port);

	    }	
	}

	static int kernel_cb_init(struct kernel_cb *cb, struct RDMABOX_session *RDMABOX_session)
	{
	    int ret = 0;
	    int i;

	    cb->RDMABOX_sess = RDMABOX_session;
	    cb->addr_type = AF_INET;
	    cb->mem = DMA;
	    cb->txdepth = RDMABOX_QUEUE_DEPTH * submit_queues + 1;
	    cb->state = IDLE;

	    cb->remote_chunk.chunk_size_g = 0;
	    cb->remote_chunk.chunk_list = (struct remote_chunk_g **)kzalloc(sizeof(struct remote_chunk_g *) * MAX_MR_SIZE_GB, GFP_KERNEL);
	    cb->remote_chunk.remote_mapped = (atomic_t *)kmalloc(sizeof(atomic_t) * MAX_MR_SIZE_GB, GFP_KERNEL);
	    cb->remote_chunk.chunk_map = (int *)kzalloc(sizeof(int) * MAX_MR_SIZE_GB, GFP_KERNEL);
	    cb->remote_chunk.evict_chunk_map = (char *)kzalloc(sizeof(char) * MAX_MR_SIZE_GB, GFP_KERNEL);
	    cb->remote_chunk.migration_chunk_map = (char *)kzalloc(sizeof(char) * MAX_MR_SIZE_GB, GFP_KERNEL);
	    for (i=0; i < MAX_MR_SIZE_GB; i++){
		atomic_set(cb->remote_chunk.remote_mapped + i, CHUNK_UNMAPPED);
		cb->remote_chunk.chunk_map[i] = -1;
		cb->remote_chunk.chunk_list[i] = (struct remote_chunk_g *)kzalloc(sizeof(struct remote_chunk_g), GFP_KERNEL); 
		cb->remote_chunk.chunk_list[i]->chunk_state = ACTIVE; 
		cb->remote_chunk.evict_chunk_map[i] = 0x00;
		cb->remote_chunk.migration_chunk_map[i] = 0x00;
	    }

	    init_waitqueue_head(&cb->sem);
	    init_waitqueue_head(&cb->remote_chunk.sem);
	    cb->remote_chunk.c_state = C_IDLE;

	    for (i=0; i < NUM_CHANNEL; i++){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
		cb->cm_id[i] = rdma_create_id(&init_net, RDMABOX_cma_event_handler, cb, RDMA_PS_TCP, IB_QPT_RC);
#else
		cb->cm_id[i] = rdma_create_id(RDMABOX_cma_event_handler, cb, RDMA_PS_TCP, IB_QPT_RC);
#endif
		if (IS_ERR(cb->cm_id[i])) {
		    ret = PTR_ERR(cb->cm_id[i]);
		    printk("rdmabox[%s]: rdma_create_id error %d\n",__func__, ret);
		    goto out;
		} 
		//#ifdef DEBUG
		printk("rdmabox[%s]: created cm_id %p\n", __func__, cb->cm_id[i]);
		//#endif
	    }

	    return 0;
out:
	    kfree(cb);
	    return ret;
	}

int find_the_best_node(struct RDMABOX_session *RDMABOX_session, int *replica_list, int num_replica, int isMig){

	    int i, j;
	    //unsigned int k;
	    int k=0;
	    int num_possible_servers=0;
	    int num_available_servers=0;
	    struct kernel_cb *tmp_cb;
	    int candidate[NUM_CB];
	    unsigned int select_history[NUM_CB];
	    unsigned int cb_index;

	    int free_mem[SERVER_SELECT_NUM];
	    int free_mem_sorted[SERVER_SELECT_NUM]; 
	    int grad[SERVER_SELECT_NUM];
	    int grad_sorted[SERVER_SELECT_NUM];
	    int rank_score[NUM_CB]; 
	    int rank_sorted[NUM_CB]; 
	    int tmp_int;
	    unsigned long tmp_ul;
#ifdef RPC_LAT_DEBUG
	    struct timeval start, end;
	    unsigned long diff;
#endif

	    // initialization
	    for (i = 0; i < SERVER_SELECT_NUM; i++){
		free_mem[i] = -1;
		free_mem_sorted[i] = NUM_CB;
		grad[i] = 0;
		grad_sorted[i] = SERVER_SELECT_NUM;
		rank_score[i] = -1;
		rank_sorted[i] = SERVER_SELECT_NUM;
	    }
	    for (i=0; i < NUM_CB ;i++){
		candidate[i] = -1;
		select_history[i] = 0;
	    }

	    // find out available servers
	    for (i=0; i<NUM_CB;i++){
		// TODO : avoid the server that has replica
		if (RDMABOX_session->cb_state_list[i] < CB_MIGRATING) {
		    candidate[num_possible_servers]=i;
		    //printk("rdmabox[%s]: candidate[%d]=%d \n",__func__,num_possible_servers,candidate[num_possible_servers]);
		    num_possible_servers++;
		}
		//printk("rdmabox[%s]: cb_state_list[%d]=%d \n",__func__,i,RDMABOX_session->cb_state_list[i]);
	    }

	    // select random nodes
	    for (i=0; i < num_possible_servers ;i++){

		if(num_possible_servers <= SERVER_SELECT_NUM){
		    cb_index = candidate[i];
#ifdef DEBUG
		    printk("rdmabox[%s]: choose candidate[%d]=%d \n",__func__,i,candidate[i]);
#endif
		}else{
		    //get_random_bytes(&k, sizeof(unsigned int));
		    get_random_bytes(&k, sizeof(int)-1);
		    k %= num_possible_servers;
		    while (select_history[k] == 1) {
			k += 1;	
			k %= num_possible_servers;
		    }
		    select_history[k] = 1;
		    cb_index = candidate[k];
#ifdef DEBUG
		    printk("rdmabox[%s]: choose random candidate[%d]=%d \n",__func__,k,candidate[k]);
#endif
		}

		tmp_cb = RDMABOX_session->cb_list[cb_index];
		if (RDMABOX_session->cb_state_list[cb_index] > CB_IDLE) { // if state is larger than cb_idle, it has been connected before.
#ifdef RPC_LAT_DEBUG
		    do_gettimeofday(&start);
#endif
		    tmp_cb->send_buf.type = QUERY;
		    post_send(tmp_cb);
		    wait_event_interruptible(tmp_cb->sem, tmp_cb->state == RECV_FREE_MEM);
#ifdef RPC_LAT_DEBUG
		    do_gettimeofday(&end);
		    diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
		    printk("lat RECV_FREE_MEM %ld usec\n",diff);
#endif
		}else { // cb_idle
		    //kernel_cb_init(tmp_cb, RDMABOX_session);
		    //rdma_connect_upper(tmp_cb, RDMABOX_session);	
		    for(j=0; j< NUM_CHANNEL; j++){
			tmp_cb->state = ROUTE_RESOLVED;
			rdma_connect_down(tmp_cb, tmp_cb->qp[j], tmp_cb->cm_id[j]);	
			wait_event_interruptible(tmp_cb->sem, tmp_cb->state == RECV_FREE_MEM);
		    }
		    RDMABOX_session->cb_state_list[cb_index] = CB_CONNECTED; //add cb_connected		
		}

		if(tmp_cb->remote_chunk.target_size_g == 0){
#ifdef DEBUG
		    printk("rdmabox[%s]: skip this node node[%d] free_mem:%d num_available_servers:%d \n",__func__,cb_index,free_mem[i],num_available_servers);
#endif
		    continue;
		}

		tmp_cb->state = AFTER_FREE_MEM;
		free_mem[num_available_servers] = tmp_cb->remote_chunk.target_size_g;
		free_mem_sorted[num_available_servers] = cb_index;
		grad[num_available_servers] = tmp_cb->remote_chunk.grad;
		grad_sorted[num_available_servers] = cb_index;
#ifdef DEBUG
		printk("rdmabox[%s]: node[%d] free_mem:%d \n",__func__,cb_index,free_mem[num_available_servers]);
#endif
		num_available_servers++;
		if(num_available_servers == SERVER_SELECT_NUM)
		    break;
	    }//for

	    // remote nodes have not enough free_mem
	    if(num_available_servers < num_replica){
#ifdef DEBUG
		printk("rdmabox[%s]: return 0. num_available_servers:%d < num_replica:%d \n",__func__,num_available_servers,num_replica);
#endif
		return 0;
	    }

	    // do sorting descent order of freemem 
	    for (i=0; i<num_available_servers-1; i++) {
		for (j=i+1; j<num_available_servers; j++) {
		    if (free_mem[i] < free_mem[j]) {
			tmp_int = free_mem[i];
			free_mem[i] = free_mem[j];
			free_mem[j] = tmp_int;
			tmp_int = free_mem_sorted[i];
			free_mem_sorted[i] = free_mem_sorted[j];
			free_mem_sorted[j] = tmp_int;
		    }
		}
	    }
	    /* 
	       for (i=0; i<num_available_servers; i++) {
	       printk("rdmabox[%s]: sorted free_mem[%d]=%d \n",__func__,i,free_mem[i]);
	       }
	     */

	     // do the sorting descent order of gradient of free memory
	    for (i=0; i<num_available_servers-1; i++) {
	        for (j=i+1; j<num_available_servers; j++) {
	             if (grad[i] < grad[j]){
	                   tmp_int = grad[i];
	                   grad[i] = grad[j];
			   grad[j] = tmp_int;
			   tmp_int = grad_sorted[i];
			   grad_sorted[i] = grad_sorted[j];
			   grad_sorted[j] = tmp_int;
		      }
		 }
            }

	    for (i=0; i<num_available_servers; i++) {
		       printk("rdmabox[%s]: sorted grad[%d]=%d \n",__func__,i,grad[i]);
	    }

	    	    // calculate scores
	    j = num_available_servers;
	    for (i=0; i<num_available_servers; i++) {
	        rank_score[free_mem_sorted[i]] = (j * FREE_MEM_WEIGHT);
	        rank_sorted[free_mem_sorted[i]] = free_mem_sorted[i];
	        //printk("rdmabox[%s]: FREE_MEM rank_score[%d]=%d \n",__func__,free_mem_sorted[i],rank_score[free_mem_sorted[i]]);
	        j--;
	    }
	    
	    j = num_available_servers;
            for (i=0; i<num_available_servers; i++) {
	       rank_score[grad_sorted[i]] += (j * GRAD_WEIGHT); //add grad score on the cb_index
	       printk("rdmabox[%s]: add grad score :%d  rank_score[%d]=%d \n",__func__,(j * GRAD_WEIGHT),grad_sorted[i],rank_score[grad_sorted[i]]);
	       j--;
	    }

	    // do the sorting descent order of rank score
	    for (i=0; i<(SERVER_SELECT_NUM-1); i++) {
	       for (j=i+1; j<SERVER_SELECT_NUM; j++) {
	           if (rank_score[i] < rank_score[j]){
	                tmp_int = rank_score[i];
	                rank_score[i] = rank_score[j];
			rank_score[j] = tmp_int;
			tmp_int = rank_sorted[i];
			rank_sorted[i] = rank_sorted[j];
			rank_sorted[j] = tmp_int;
			//printk("rdmabox[%s]: swap rank_score[%d]=%d rank_sorted[%d]=%d \n",__func__,i,rank_score[i],i,rank_sorted[i]);
		   }
		}
	    }

            for (i=0; i<num_available_servers; i++) {
	        printk("rdmabox[%s]: FINAL rank_score[%d]=%d rank_sorted[%d]=%d\n",__func__,i,rank_score[i],i,rank_sorted[i]);
	    }

	    j = 0;
	    for(i=0; i<num_replica; i++){
		replica_list[j] = rank_sorted[i];
#ifdef DEBUG
		printk("rdmabox[%s]: selected node : replica[%d]=%d NUM_REPLICA:%d\n",__func__,j,replica_list[j],num_replica);
#endif
		j++;
	    }

	    return j;
	}

	void post_setup_connection(struct RDMABOX_session *RDMABOX_session, struct kernel_cb *tmp_cb, int cb_index)
	{
	    int i;
	    char name[2];
	    /*
	       for(i=0; i< submit_queues; i++){
	       rdma_freepool_post_init(RDMABOX_session, tmp_cb->rdma_write_freepool_list[i],RDMABOX_PAGE_SIZE * RDMA_WR_BUF_LEN, 1);
	       rdma_freepool_post_init(RDMABOX_session, tmp_cb->rdma_read_freepool_list[i], RDMABOX_PAGE_SIZE * PAGE_CLUSTER, 0);
	       }
	     */
	    memset(name, '\0', 2);
	    name[0] = (char)((cb_index/26) + 97);
	    tmp_cb->remote_chunk.evict_handle_thread = kthread_create(migration_handler, tmp_cb, name);
	    wake_up_process(tmp_cb->remote_chunk.evict_handle_thread);	
	}

	int RDMABOX_single_chunk_map(struct RDMABOX_session *RDMABOX_session, int select_chunk, struct kernel_cb **replica_cb_list)
	{
	    struct kernel_cb *tmp_cb;
	    int cb_index;
	    int replica_list[NUM_REPLICA];
	    int rst=0;
	    int i;
#ifdef RPC_LAT_DEBUG
	    struct timeval start, end;
	    unsigned long diff;
#endif

#ifdef DEBUG
	    printk("rdmabox[%s]: find_the_best_node \n",__func__);
#endif
retry:
	    rst = find_the_best_node(RDMABOX_session,replica_list,NUM_REPLICA,0);
	    if(rst < NUM_REPLICA){
		printk("rdmabox[%s]: fail to find_the_best_node num_found:%d\n",__func__,rst);
		goto retry;
		//return rst;
	    }

	    rst=0;
	    for(i=0; i<NUM_REPLICA; i++){
		cb_index = replica_list[i];
#ifdef DEBUG
		printk("rdmabox[%s]: replica_index[%d]=%d \n",__func__,i,cb_index);
#endif
		tmp_cb = RDMABOX_session->cb_list[cb_index];
		if (RDMABOX_session->cb_state_list[cb_index] == CB_CONNECTED){ 
		    post_setup_connection(RDMABOX_session, tmp_cb, cb_index);
		}
		RDMABOX_session->mapped_cb_num += 1;

		tmp_cb->send_buf.type = BIND_SINGLE;
		tmp_cb->send_buf.size_gb = select_chunk; 
		//tmp_cb->send_buf.cb_index = i; //use this for marking for replica index
		tmp_cb->send_buf.replica_index = i; //use this for marking for replica index
#ifdef RPC_LAT_DEBUG
		do_gettimeofday(&start);
#endif
		post_send(tmp_cb);
#ifdef DEBUG
		printk("rdmabox[%s]: Send BIND_SINGLE swapspace[%d], replica_index[%d] to node[%d] \n",__func__,select_chunk,i,cb_index);
#endif
		wait_event_interruptible(tmp_cb->sem, tmp_cb->state == RECV_INFO_SINGLE);
#ifdef RPC_LAT_DEBUG
		do_gettimeofday(&end);
		diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
		printk("lat RECV_INFO_SINGLE %ld usec\n",diff);
#endif
		replica_cb_list[i] = tmp_cb;
		rst++;
	    }//replica loop

	    atomic_set(&RDMABOX_session->rdma_on, DEV_RDMA_ON); 

	    //chunk ops window pos update
	    //RDMABOX_session->pos_wr_hist = (++RDMABOX_session->pos_wr_hist)%3;
	    //RDMABOX_session->pos_rd_hist = (++RDMABOX_session->pos_rd_hist)%3;

	    return rst;
	}

	struct kernel_cb* RDMABOX_migration_chunk_map(struct RDMABOX_session *RDMABOX_session, int client_evict_chunk_index, struct kernel_cb *old_cb, int src_cb_index, int replica_index)
	{
	    struct kernel_cb *tmp_cb;
	    int cb_index;
	    int i;
	    int src_chunk_index;
	    int replica_list[NUM_REPLICA];
	    int rst=0;
#ifdef RPC_LAT_DEBUG
	    struct timeval start, end;
	    unsigned long diff;
#endif

#ifdef DEBUG
	    printk("rdmabox[%s]: find_the_best_node \n",__func__);
#endif
	    rst = find_the_best_node(RDMABOX_session,replica_list,1,1);
	    if(rst < 1){
		printk("rdmabox[%s]: fail to find_the_best_node num_found:%d\n",__func__,rst);
		return NULL;
	    }

	    rst=0;
	    cb_index = replica_list[0];

	    /*
	    // this is rare. this should be fixed and removed
	    i=1;
	    while(cb_index == src_cb_index){
	    cb_index = replica_list[(i%NUM_REPLICA)];
	    printk("rdmabox[%s]: selected node[%d] cannot be the src_node[%d] \n",__func__,cb_index,src_cb_index);
	    i++;
	    }
	     */
	    // TODO : should know this is pri or sec
	    src_chunk_index = RDMABOX_session->mapping_swapspace_to_chunk[replica_index][client_evict_chunk_index];

	    tmp_cb = RDMABOX_session->cb_list[cb_index];
	    if (RDMABOX_session->cb_state_list[cb_index] == CB_CONNECTED){ 
		post_setup_connection(RDMABOX_session, tmp_cb, cb_index);
	    }

	    printk("rdmabox[%s]: Send bind migration MSG for swapspace[%d](chunk[%d] on srcnode[%d]) to node[%d] \n",__func__,client_evict_chunk_index,src_chunk_index,src_cb_index,cb_index);

	    // evict addr and rkey at src addr
	    for (i=0; i<MAX_MR_SIZE_GB; i++){
		if (old_cb->remote_chunk.migration_chunk_map[i] == 'm'){
		    tmp_cb->send_buf.rkey[i] = old_cb->remote_chunk.chunk_list[i]->remote_rkey;
		    tmp_cb->send_buf.buf[i] = old_cb->remote_chunk.chunk_list[i]->remote_addr;
		    break;
		}else{
		    tmp_cb->send_buf.rkey[i] = 0;
		}
	    }

	    tmp_cb->send_buf.type = BIND_MIGRATION;
	    tmp_cb->send_buf.size_gb = client_evict_chunk_index;
	    tmp_cb->send_buf.chunk_index = src_chunk_index;
	    tmp_cb->send_buf.cb_index = src_cb_index;
	    tmp_cb->send_buf.replica_index = replica_index;
#ifdef RPC_LAT_DEBUG
	    do_gettimeofday(&start);
#endif
	    post_send(tmp_cb);

	    wait_event_interruptible(tmp_cb->sem, tmp_cb->state == RECV_INFO_MIG);
#ifdef RPC_LAT_DEBUG
	    do_gettimeofday(&end);
	    diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
	    printk("lat RECV_INFO_MIG %ld usec node %d\n",diff,src_cb_index);
#endif
	    //chunk ops window pos update
	    //RDMABOX_session->pos_wr_hist = (++RDMABOX_session->pos_wr_hist)%3;
	    //RDMABOX_session->pos_rd_hist = (++RDMABOX_session->pos_rd_hist)%3;

	    return tmp_cb;
	}

	int RDMABOX_session_create(const char *portal, struct RDMABOX_session *RDMABOX_session)
	{
	    int i, j, ret;
	    char name[20];
	    /*
	       char thread_name[10]="rsender";
	       char thread_name2[10]="dsender";
	       char thread_name3[10]="dreader";
	     */
	    char doorbell_write_name[10]="db_wr";
	    char doorbell_read_name[10]="db_rd";

	    struct task_struct *ts;
	    struct kernel_cb *cb;
	    struct kernel_cb *replica_cb_list[NUM_REPLICA];

	    printk("rdmabox[%s]: In RDMABOX_session_create() with portal: %s\n",__func__, portal);

	    spin_lock_init(&RDMABOX_session->cb_lock); 

	    memcpy(RDMABOX_session->portal, portal, strlen(portal));
	    printk("rdmabox[%s]: %s\n",__func__,RDMABOX_session->portal);

	    portal_parser(RDMABOX_session);
#ifdef FLOW_CONTROL
	    atomic_set(&num_outstanding,0);
#endif

#if defined(USE_MULTI_SCQ_BUSY_POLL)
	    atomic_set(&RDMABOX_session->curr_scq_index,0);
	    atomic_set(&RDMABOX_session->curr_thread_index,0);
	    for (i=0; i<NUM_SCQ; i++) {
		RDMABOX_session->scq[i] = NULL;
	    }
#endif

	    RDMABOX_session->pos_wr_hist = 0; 
	    RDMABOX_session->pos_rd_hist = 0; 
	    RDMABOX_session->capacity_g = STACKBD_SIZE_G; 
	    RDMABOX_session->capacity = (unsigned long long)STACKBD_SIZE_G * ONE_GB;
	    RDMABOX_session->mapped_cb_num = 0;
	    RDMABOX_session->mapped_capacity = 0;
	    RDMABOX_session->pos_wr_hist = 0; 
	    RDMABOX_session->pd = NULL; 
	    //cb init
	    RDMABOX_session->cb_list = (struct kernel_cb **)kzalloc(sizeof(struct kernel_cb *) * RDMABOX_session->cb_num, GFP_KERNEL);	
	    RDMABOX_session->cb_state_list = (enum cb_state *)kzalloc(sizeof(enum cb_state) * RDMABOX_session->cb_num, GFP_KERNEL);
	    for (i=0; i<RDMABOX_session->cb_num; i++) {
		RDMABOX_session->cb_state_list[i] = CB_IDLE;	
		RDMABOX_session->cb_list[i] = kzalloc(sizeof(struct kernel_cb), GFP_KERNEL);
		RDMABOX_session->cb_list[i]->port = htons(RDMABOX_session->portal_list[i].port);
		in4_pton(RDMABOX_session->portal_list[i].addr, -1, RDMABOX_session->cb_list[i]->addr, -1, NULL);
		RDMABOX_session->cb_list[i]->cb_index = i;

		RDMABOX_session->cb_list[i]->qplist = (struct ib_qp **)kmalloc(sizeof(struct ib_qp *) * submit_queues, GFP_KERNEL | __GFP_ZERO);
		if (!RDMABOX_session->cb_list[i]->qplist){
		    printk("mempool[%s]: Fail to create qp list  \n",__func__);
		    return 1;
		}
		kernel_cb_init(RDMABOX_session->cb_list[i], RDMABOX_session);
		rdma_connect_upper(RDMABOX_session->cb_list[i], RDMABOX_session);	
		// map qp
		for (j=0; j<submit_queues; j++) {
		    RDMABOX_session->cb_list[i]->qplist[j] = RDMABOX_session->cb_list[i]->qp[j%NUM_CHANNEL];
		}
	    }//for

	    for (i = 0; i < NUM_REPLICA; i++) {
		RDMABOX_session->cb_index_map[i] = kzalloc(sizeof(atomic_t) * RDMABOX_session->capacity_g, GFP_KERNEL);
		RDMABOX_session->mapping_swapspace_to_chunk[i] = (int*)kzalloc(sizeof(int) * RDMABOX_session->capacity_g, GFP_KERNEL);
		//RDMABOX_session->unmapped_chunk_list = (int*)kzalloc(sizeof(int) * RDMABOX_session->capacity_g, GFP_KERNEL);
		//RDMABOX_session->free_chunk_index = RDMABOX_session->capacity_g - 1;
		for (j = 0; j < RDMABOX_session->capacity_g; j++){
		    atomic_set(RDMABOX_session->cb_index_map[i] + j, NO_CB_MAPPED);
		    //RDMABOX_session->unmapped_chunk_list[i] = RDMABOX_session->capacity_g-1-i;
		    RDMABOX_session->mapping_swapspace_to_chunk[i][j] = -1;
		}
	    }
	    /*
	    //IS-connection create
	    RDMABOX_session->RDMABOX_conns = (struct RDMABOX_connection **)kzalloc(submit_queues * sizeof(*RDMABOX_session->RDMABOX_conns), GFP_KERNEL);
	    if (!RDMABOX_session->RDMABOX_conns) {
	    printk("rdmabox[%s]: failed to allocate IS connections array\n",__func__);

	    ret = -ENOMEM;
	    goto err_destroy_portal;
	    }
	    for (i = 0; i < submit_queues; i++) {
	    ret = RDMABOX_create_conn(RDMABOX_session, i, &RDMABOX_session->RDMABOX_conns[i]);
	    if (ret)
	    goto err_destroy_conns;
	    }
	    atomic_set(&RDMABOX_session->rdma_on, DEV_RDMA_OFF);
	     */

	    // connect swap space 0
	    cb = get_cb(RDMABOX_session, 0, replica_cb_list);

	    // allocate ctx mr pool
	    //for (i=0; i<RDMABOX_session->cb_num; i++) {
	    for (i=0; i<NUM_POOL; i++) {
		RDMABOX_session->cb_list[i]->rdma_write_freepool_list = (struct rdma_freepool_s **)kmalloc(sizeof(struct rdma_freepool_s *) * submit_queues, GFP_KERNEL | __GFP_ZERO);
		if (!RDMABOX_session->cb_list[i]->rdma_write_freepool_list){
		    printk("mempool[%s]: Fail to create rdma_write_freepool_list  \n",__func__);
		    return 1;
		}
		for (j=0; j<submit_queues; j++) {
		    //if(rdma_freepool_create(RDMABOX_session, &RDMABOX_session->cb_list[i]->rdma_write_freepool_list[j], (NUM_WR_CTX / RDMABOX_session->cb_num), (NUM_WR_MR / RDMABOX_session->cb_num), RDMABOX_PAGE_SIZE * RDMA_WR_BUF_LEN))
		    if(rdma_freepool_create(RDMABOX_session, &RDMABOX_session->cb_list[i]->rdma_write_freepool_list[j], NUM_WR_CTX, NUM_WR_MR, RDMABOX_PAGE_SIZE * RDMA_WR_BUF_LEN))
			panic("fail to create rdma write freepool");
		    rdma_freepool_post_init(RDMABOX_session, RDMABOX_session->cb_list[i]->rdma_write_freepool_list[j],RDMABOX_PAGE_SIZE * RDMA_WR_BUF_LEN, 1);
		}
	    }
	    //for (i=0; i<RDMABOX_session->cb_num; i++) {
	    for (i=0; i<NUM_POOL; i++) {
		RDMABOX_session->cb_list[i]->rdma_read_freepool_list = (struct rdma_freepool_s **)kmalloc(sizeof(struct rdma_freepool_s *) * submit_queues, GFP_KERNEL | __GFP_ZERO);
		if (!RDMABOX_session->cb_list[i]->rdma_read_freepool_list){
		    printk("mempool[%s]: Fail to create rdma_read_freepool_list  \n",__func__);
		    return 1;
		}
		for (j=0; j<submit_queues; j++) {
		    //if(rdma_freepool_create(RDMABOX_session, &RDMABOX_session->cb_list[i]->rdma_read_freepool_list[j], (NUM_RD_CTX / RDMABOX_session->cb_num), (NUM_RD_MR / RDMABOX_session->cb_num), RDMABOX_PAGE_SIZE * PAGE_CLUSTER))
		    if(rdma_freepool_create(RDMABOX_session, &RDMABOX_session->cb_list[i]->rdma_read_freepool_list[j], NUM_RD_CTX, NUM_RD_MR, RDMABOX_PAGE_SIZE * PAGE_CLUSTER))
			panic("fail to create rdma read freepool list");
		    rdma_freepool_post_init(RDMABOX_session, RDMABOX_session->cb_list[i]->rdma_read_freepool_list[j], RDMABOX_PAGE_SIZE * PAGE_CLUSTER, 0);
		}
	    }

#ifdef PRE_ALLOC
	    for (i = 0; i < STACKBD_SIZE_G; i++) {
		cb = get_cb(RDMABOX_session, i, replica_cb_list);
		//printk("rdmabox[%s]:primary cb->index:%d replica_cb_list[0]->index:%d \n",__func__,cb->cb_index,replica_cb_list[0]->cb_index);
		msleep(1);
		if(!cb){
		    printk("rdmabox[%s]: cb is null \n", __func__);
		    continue;
		}
	    }
#endif

	    // start remote sender thread
	    /*
#ifdef USE_DOORBELL_WRITE
printk("rdmabox[%s]: create doorbell batch write thread %s \n",__func__,doorbell_write_name);
RDMABOX_session->doorbell_batch_write_thread = kthread_create(doorbell_batch_write_fn, RDMABOX_session, doorbell_write_name);
wake_up_process(RDMABOX_session->doorbell_batch_write_thread);      
#endif

#ifdef USE_DOORBELL_READ
printk("rdmabox[%s]: create doorbell batch read thread %s \n",__func__,doorbell_read_name);
RDMABOX_session->doorbell_batch_read_thread = kthread_create(doorbell_batch_read_fn, RDMABOX_session, doorbell_read_name);
wake_up_process(RDMABOX_session->doorbell_batch_read_thread);      
#endif
	     */
	    /*
	    // start remote sender thread
	    printk("rdmabox[%s]: create sender thread %s \n",__func__,thread_name);
	    RDMABOX_session->remote_sender_thread = kthread_create(remote_sender_fn, RDMABOX_session, thread_name);
	    wake_up_process(RDMABOX_session->remote_sender_thread);      

	    // start direct sender thread
	    printk("rdmabox[%s]: create direct sender thread %s \n",__func__,thread_name2);
	    RDMABOX_session->direct_sender_thread = kthread_create(batch_write_fn, RDMABOX_session, thread_name2);
	    wake_up_process(RDMABOX_session->direct_sender_thread);      

	    // start direct reader thread
	    printk("rdmabox[%s]: create direct reader thread %s \n",__func__,thread_name3);
	    RDMABOX_session->direct_reader_thread = kthread_create(batch_read_fn, RDMABOX_session, thread_name3);
	    wake_up_process(RDMABOX_session->direct_reader_thread);      
	     */

	    /*
	    // start remote sender thread
	    printk("rdmabox[%s]: create sender thread %s cpu:%d \n",__func__,thread_name,0);
	    ts = kthread_create(remote_sender_fn, RDMABOX_session, thread_name);
	    kthread_bind(ts, 0); // bind with cpu 0
	    if (!IS_ERR(ts)) {
	    wake_up_process(ts);
	    } else {
	    printk(KERN_ERR "Failed to bind thread to CPU %d\n", i);
	    }
	     */
	    return 0;
	    /*
err_destroy_conns:
for (j = 0; j < i; j++) {
RDMABOX_destroy_conn(RDMABOX_session->RDMABOX_conns[j]);
RDMABOX_session->RDMABOX_conns[j] = NULL;
}
kfree(RDMABOX_session->RDMABOX_conns);
	     */
err_destroy_portal:

return ret;
}

void RDMABOX_session_destroy(struct RDMABOX_session *RDMABOX_session)
{
    mutex_lock(&g_lock);
    list_del(&RDMABOX_session->list);
    mutex_unlock(&g_lock);

    printk("rdmabox[%s]: \n", __func__);

    RDMABOX_destroy_session_devices(RDMABOX_session);
}


