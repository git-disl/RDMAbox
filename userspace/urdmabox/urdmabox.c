/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
 *
 * Copyright (c) 2005 Ammasso, Inc. All rights reserved.
 * Copyright (c) 2006-2009 Open Grid Computing, Inc. All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#include <stdbool.h>
#include <getopt.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <pthread.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <linux/kernel.h>
#include <sys/time.h>
#include <time.h>
#include <endian.h>
#include <byteswap.h>

#include "urdmabox.h"
#include "debug.h"
#include "rpg_mempool.h"
#include "radixtree.h"

int put_wr_req_item(void *tmp)
{
    if (lfqueue_enq(session.wr_req_q, tmp) < 0){
	syslog(LOG_NOTICE,"mempool[%s]: Fail to put wr req item \n",__func__);
	return 1;
    }
    //DEBUG_LOG(LOG_NOTICE,"[%s]:put wr item count=%u \n", __func__,lfqueue_size(session.wr_req_q));
    return 0;
}

int get_wr_req_item(void **tmp)
{
    if (lfqueue_size(session.wr_req_q) == 0){
	return 1;
    }

    *tmp = (struct urdmabox_req*)lfqueue_deq_must(session.wr_req_q);
    if(!tmp){
	return 1;
    }
    //DEBUG_LOG(LOG_NOTICE,"[%s]:get wr item count=%u \n", __func__,lfqueue_size(session.wr_req_q));
    return 0;
}

int put_rd_req_item(void *tmp)
{
    if (lfqueue_enq(session.rd_req_q, tmp) < 0){
	syslog(LOG_NOTICE,"mempool[%s]: Fail to put wr req item \n",__func__);
	return 1;
    }
    //DEBUG_LOG(LOG_NOTICE,"[%s]:put rd item count=%u \n", __func__,lfqueue_size(session.rd_req_q));
    return 0;
}

int get_rd_req_item(void **tmp)
{
    if (lfqueue_size(session.rd_req_q) == 0){
	return 1;
    }

    *tmp = lfqueue_deq_must(session.rd_req_q);
    if(!tmp){
	return 1;
    }
    //DEBUG_LOG(LOG_NOTICE,"[%s]:get rd item count=%u \n", __func__,lfqueue_size(session.rd_req_q));
    return 0;
}

int registercallback(char *func_str, void (*ptr)(void *))
{
    if(!strcmp("write_done",func_str)){
	DEBUG_LOG(LOG_NOTICE,"done register %s\n",func_str);
	ops.write_done = ptr;
    }
    else if(!strcmp("read_done",func_str)){
	DEBUG_LOG(LOG_NOTICE,"done register %s\n",func_str);
	ops.read_done = ptr;
    }
}

void atomic_init(struct atomic_t *m)
{
    pthread_spin_init(&m->atomic_lock,0);
    m->value = -1;
}

void atomic_set(struct atomic_t *m, int val)
{
    pthread_spin_lock(&m->atomic_lock);
    m->value = val;
    pthread_spin_unlock(&m->atomic_lock);
}

int atomic_inc_return(struct atomic_t *m)
{
    int rst;
    pthread_spin_lock(&m->atomic_lock);
    m->value = m->value + 1;
    rst = m->value;
    pthread_spin_unlock(&m->atomic_lock);
    //DEBUG_LOG(LOG_NOTICE,"atomic_inc_return:%d\n",rst);
    return rst;
}

int atomic_dec(struct atomic_t *m)
{
    int rst;
    pthread_spin_lock(&m->atomic_lock);
    m->value = m->value - 1;
    rst = m->value;
    pthread_spin_unlock(&m->atomic_lock);
    //DEBUG_LOG(LOG_NOTICE,"atomic_dec:%d\n",rst);
    return rst;
}

int atomic_read(struct atomic_t *m)
{
    int res;
    pthread_spin_lock(&m->atomic_lock);
    res = m->value;
    pthread_spin_unlock(&m->atomic_lock);

    return res;
}

void post_send(struct krping_cb *cb)
{
    int ret=0;
    struct ibv_send_wr *bad_wr;

    ret = ibv_post_send(cb->qp, &cb->sq_wr, &bad_wr);
    if (ret) {
	syslog(LOG_NOTICE,stderr, "post send error %d\n", ret);
	exit(-1);
    }
}

void post_receive(struct krping_cb *cb)
{
    int ret=0;
    struct ibv_recv_wr *bad_wr;

    ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
    if (ret) {
	syslog(LOG_NOTICE,stderr, "ibv_post_recv failed: %d\n", ret);
	exit(-1);
    }
}

void mr_freepool_post_init(struct session *session, struct rdma_freepool_s *pool, int buf_size)
{
    int i;
    struct rdma_mr *mr;

    for(i=0; i<pool->cap_nr_mr; i++){

	mr = pool->mr_elements[i];
	mr->rdma_mr = ibv_reg_mr(session->pd, mr->rdma_buf, buf_size,
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
	if (!mr->rdma_mr) {
	    syslog(LOG_NOTICE,"rdmabox[%s]: rdma_mr ibv_reg_mr failed buf_size:%d\n",__func__,buf_size);
	    free(mr->rdma_buf);
	    break;
	}
    }

    pool->init_done = 1;   
}

void rdma_freepool_post_init(struct session *session, struct krping_cb *cb, struct rdma_freepool_s *pool, int buf_size, int isWrite)

{
    int i;
    struct rdma_ctx *ctx;

    for(i=0; i<pool->cap_nr_mr; i++){

	ctx = pool->mr_elements[i];

	ctx->mr->rdma_mr = ibv_reg_mr(session->pd, ctx->mr->rdma_buf, buf_size,
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
	if (!ctx->mr->rdma_mr) {
	    syslog(LOG_NOTICE,"rdmabox[%s]: rdma_mr ibv_reg_mr failed buf_size:%d\n",__func__,buf_size);
	    free(ctx->mr->rdma_buf);
	    continue;
	}

	ctx->rdma_sgl.addr = (uintptr_t) ctx->mr->rdma_buf;
	ctx->rdma_sgl.length = sizeof(ctx->mr->rdma_buf);
	ctx->rdma_sgl.lkey = ctx->mr->rdma_mr->lkey;

	ctx->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
	ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl;
	ctx->rdma_sq_wr.num_sge = 1;
	ctx->rdma_sq_wr.next = NULL;
	ctx->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;

	if(isWrite){
	    ctx->rdma_sq_wr.opcode = IBV_WR_RDMA_WRITE;
	    //ctx->rdma_sq_wr.wr_id = uint64_from_ptr(RPC_MSG_POOL_SIZE);
	    //ctx->rdma_sq_wr.imm_data = imm;

	}else{//read
	    ctx->rdma_sq_wr.opcode = IBV_WR_RDMA_READ;
	}//Read
    }//while

    pool->init_done = 1;   
}

int rdma_read(struct session *sess, struct krping_cb *cb, int rb_index, struct remote_block *rb, unsigned long offset, int block_index, struct rdma_ctx *ctx)
{
    int ret;
    int err;
    struct ibv_send_wr *bad_wr;
    struct rdma_mr *mr;

    ctx->rb_index = rb_index; //rb_index in cb
    ctx->cb = cb;
    //atomic_set(&ctx->in_flight, CTX_R_IN_FLIGHT);

    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: block_index=%d rb_index:%d, mod:%d LOCAL_BLOCKS_PER_REMOTE_BLOCK:%d\n",__func__, block_index, rb_index, block_index % LOCAL_BLOCKS_PER_REMOTE_BLOCK, LOCAL_BLOCKS_PER_REMOTE_BLOCK);
    ctx->rdma_sq_wr.wr.rdma.rkey = rb->remote_rkey;
    ctx->rdma_sq_wr.wr.rdma.remote_addr = rb->remote_addr + sizeof(struct remote_block_header) + ((block_index % LOCAL_BLOCKS_PER_REMOTE_BLOCK) * LOCAL_BLOCK_SIZE) + offset;

    if(ctx->isDynamic){
	ctx->rdma_sgl.addr = (uint64_t) (unsigned long) ctx->dynamic_mr.rdma_buf;
	ctx->rdma_sgl.lkey = ctx->dynamic_mr.rdma_mr->lkey;
    }else{
	ctx->rdma_sgl.addr = (uint64_t) (unsigned long) ctx->mr->rdma_buf;
	ctx->rdma_sgl.lkey = ctx->mr->rdma_mr->lkey;
    }
    ctx->rdma_sgl.length = ctx->len;

    ctx->rdma_sq_wr.opcode = IBV_WR_RDMA_READ;
    ctx->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
    //ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl;
    //ctx->rdma_sq_wr.num_sge = 1;
    ctx->rdma_sq_wr.next = NULL;
    ctx->rdma_sq_wr.wr_id = uint64_from_ptr(ctx);

    ret = ibv_post_send(cb->qp, &ctx->rdma_sq_wr, &bad_wr);
    if (ret) {
	syslog(LOG_NOTICE,"rdmabox[%s]: post write error. ret=%d, block_index=%d\n",__func__, ret, block_index);
	return ret;
    }

    return 0;
}

int rdma_write(struct session *sess, struct krping_cb *cb,  int block_index, unsigned long offset, int replica_index, struct rdma_ctx *ctx)
{
    int ret;
    struct ibv_send_wr *bad_wr;	
    int rb_index;
    int lbg_index = block_index / LOCAL_BLOCKS_PER_REMOTE_BLOCK;
    struct remote_block *rb;

    rb_index = sess->mapping_lbg_to_rb[replica_index][lbg_index];
    rb = cb->rbh.remote_block[rb_index];

    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: block_index=%d lbg_index=%d mod:%d rb_index:%d\n",__func__, block_index, lbg_index, block_index % LOCAL_BLOCKS_PER_REMOTE_BLOCK,rb_index);

    ctx->cb = cb;
    ctx->rb = rb;
    ctx->rb_index = rb_index;
    ctx->rdma_sq_wr.wr.rdma.rkey = rb->remote_rkey;
    ctx->rdma_sq_wr.wr.rdma.remote_addr = rb->remote_addr + sizeof(struct remote_block_header) + ((block_index % LOCAL_BLOCKS_PER_REMOTE_BLOCK) * LOCAL_BLOCK_SIZE) + offset;

    if(ctx->isDynamic){
	ctx->rdma_sgl.addr = (uint64_t) (unsigned long) ctx->dynamic_mr.rdma_buf;
	ctx->rdma_sgl.lkey = ctx->dynamic_mr.rdma_mr->lkey;
    }else{
	ctx->rdma_sgl.addr = (uint64_t) (unsigned long) ctx->mr->rdma_buf;
	ctx->rdma_sgl.lkey = ctx->mr->rdma_mr->lkey;
    }
    ctx->rdma_sgl.length = ctx->len;

    ctx->rdma_sq_wr.opcode = IBV_WR_RDMA_WRITE;
    ctx->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
    //ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl;
    //ctx->rdma_sq_wr.num_sge = 1;
    ctx->rdma_sq_wr.next = NULL;
    ctx->rdma_sq_wr.wr_id = uint64_from_ptr(ctx);

    ret = ibv_post_send(cb->qp, &ctx->rdma_sq_wr, &bad_wr);
    if (ret) {
	syslog(LOG_NOTICE,"rdmabox[%s]: post write error. ret=%d, block_index=%d lbg_index=%d\n",__func__, ret, block_index, lbg_index);
	return ret;
    }
    return 0;
}

// this is for direct read
int read_remote_direct(struct urdmabox_req *req)
{
    int i,j;
    int rst, retval;
    struct rdma_ctx *ctx;
    struct rdma_mr *mr;
    struct krping_cb *cb;
    struct remote_block *rb;
    int rb_index;
    struct krping_cb *replica_cb_list[NUM_REPLICA];
    unsigned long rb_offset;
    int cb_index;
    int block_index = req->block_index;
    char* buf = req->buf;
    unsigned long offset = req->offset;
    //unsigned long len = req->size;
    int len = req->size;
    int lbg_index = block_index / LOCAL_BLOCKS_PER_REMOTE_BLOCK;

    for(i=0; i<NUM_REPLICA; i++){
	cb_index = atomic_read(session.cb_index_map[i] + lbg_index);
	if(cb_index == NO_CB_MAPPED){
	    rst = 1;
	    continue;
	}
	cb = session.cb_list[cb_index];

	rb_index = session.mapping_lbg_to_rb[i][lbg_index];
	rb = cb->rbh.remote_block[rb_index];

	DEBUG_LOG(LOG_NOTICE, "[%s] buf:%p size:%ld, offset:%ld block_index:%d, lbg_index:%d rb_index:%d cb_index:%d \n",__func__, buf, len, (long)offset, block_index, lbg_index,rb_index, cb_index);
	//TODO
	/*
	// check if READONLY or NONE_ALLOWED
	if(rb->rb_state == READONLY || rb->rb_state == NONE_ALLOWED){
	DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: this rb is READONLY or NONE_ALLOWED.\n",__func__);
	continue;
	}
	 */
	//TODO : bitmap mark when write is done
	//rst = bitmap_test(rb->bitmap_g, bitmap_i);
	//if (rst){ //remote recorded

retry:
	ctx = get_ctx_freepool();
	if (!ctx){
	    //syslog(LOG_NOTICE,"rdmabox[%s]: try to get ctx[%d] \n",__func__,j);
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: try to get ctx[%d] \n",__func__,j);
	    goto retry;
	}

	//#ifndef DYNAMIC_MR_REG
	if( len < 950272 ){ // 928KB
	    DEBUG_LOG(LOG_NOTICE,"use Pre-MR\n");
retry_mr:
	    get_mr_freepool(session.mr_freepool, &mr);
	    if(!mr){
		//syslog(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
		DEBUG_LOG(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
		goto retry_mr;
	    }
	    ctx->mr = mr;
	    ctx->isDynamic = 0;
	    ctx->req_buf = buf;
	    ctx->reqlist[0] = req;
	    ctx->numreq = 1;
	    ctx->sending_item = NULL;
	    //#else
	}else{
	    DEBUG_LOG(LOG_NOTICE,"use Dynamic-MR\n");
	    ctx->dynamic_mr.rdma_buf = buf;
	    ctx->dynamic_mr.rdma_mr = ibv_reg_mr(session.pd, buf, len,
		    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
	    if (!ctx->dynamic_mr.rdma_mr) {
		syslog(LOG_NOTICE,stderr, "rdma_buf reg_mr failed\n");
		rst = 1;
		continue;
	    }
	    ctx->isDynamic = 1;
	    ctx->reqlist[0] = req;
	    ctx->numreq = 1;
	    ctx->sending_item = NULL;
	    //#endif
	} //else
	ctx->len = len;
	ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl;
	ctx->rdma_sq_wr.num_sge = 1;

	// In read, remote_read() is deprecated. Call rdma_read() directly.
	retval = rdma_read(&session, cb, rb_index, rb, offset, block_index, ctx); 
	if (retval) {
	    syslog(LOG_NOTICE,"rdmabox[%s]: failed to read from remote\n",__func__);
	    rst = 1;
	    continue;
	}
	return 0;
	/*
	   }else{
	   rst = 1;
	   }
	 */

    }//for


    return 0;
}

int remote_write(struct session *sess, struct rdma_ctx **ctx, struct krping_cb **replica_cb_list, int block_index, unsigned long offset)
{
    int retval = 0;
    int i,j,k;
    int ret;

    for(j=0; j<NUM_REPLICA; j++){
	if(j>0){
retry:
	    ctx[j] = get_ctx_freepool();
	    if (!ctx[j]){
		//syslog(LOG_NOTICE,"rdmabox[%s]: try to get ctx[%d] \n",__func__,j);
		DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: try to get ctx[%d] \n",__func__,j);
		goto retry;
	    }
	    ctx[j]->mr = ctx[0]->mr;
	    ctx[j]->len = ctx[0]->len;
	}//j>0
	//atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	ctx[j]->sending_item = NULL;
	ctx[j]->replica_index = j;
#ifdef DYNAMIC_MR_REG
	DEBUG_LOG(LOG_NOTICE,"use Dynamic-MR\n");
	ctx[j]->isDynamic = 1;
	/*
	   for(i=0; i<ctx[j]->numreq; i++){
	   ctx[j]->reqlist[i];
	   }
	   ctx[0]->rdma_sgl_wr[i].addr = 
	   ctx[0]->rdma_sgl_wr[i].length = ib_sg_dma_len(IS_sess->pd->device, sg);
	   ctx[0]->rdma_sgl_wr[i].lkey = replica_cb_list[0]->qp[0]->device->local_dma_lkey;
	 */
	ctx[j]->dynamic_mr.rdma_buf = ctx[j]->reqlist[0]->buf;
	ctx[j]->dynamic_mr.rdma_mr = ibv_reg_mr(session.pd, ctx[j]->reqlist[0]->buf, ctx[j]->len,
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
	if (!ctx[j]->dynamic_mr.rdma_mr) {
	    syslog(LOG_NOTICE,stderr, "rdma_buf reg_mr failed\n");
	    ret = errno;
	}

#else
	ctx[j]->isDynamic = 0;
#endif
	ctx[j]->prime_ctx = ctx[0];

	ctx[j]->rdma_sq_wr.sg_list = &ctx[j]->rdma_sgl;
	ctx[j]->rdma_sq_wr.num_sge = 1;
    }//replica forloop

    //TODO disk backup
    /*
#ifdef DISKBACKUP
struct bio *cloned_bio;
cloned_bio = create_bio_copy(ctx[0]->reqlist[0]->bio);
stackbd_bio_generate(ctx[0], cloned_bio, ctx[0]->len, start_idx_tmp_first );
    //stackbd_bio_generate_batch(ctx[0], start_idx_tmp_first);
#endif
     */
    // send out
    for(j=0; j<NUM_REPLICA; j++){
	ret =  rdma_write(&session, replica_cb_list[j], block_index, offset, j, ctx[j]);
	if (ret){
	    return ret;
	}
    }

    return 0;
}

// this is for direct write to remote
int write_remote_direct(struct urdmabox_req *req)
{
    int i,j;
    int ret;
    struct rdma_ctx *ctx[NUM_REPLICA];
    struct rdma_mr *mr;
    struct krping_cb *cb;
    struct remote_block *rb;
    int rb_index;
    struct krping_cb *replica_cb_list[NUM_REPLICA];
    int block_index = req->block_index;
    char *buf = req->buf;
    unsigned long offset = req->offset;
    //unsigned long len = req->size;
    int len = req->size;

    int lbg_index = block_index / LOCAL_BLOCKS_PER_REMOTE_BLOCK;

    //find primary cb
    cb = get_cb(&session, lbg_index, replica_cb_list);
    if(!cb){
	syslog(LOG_NOTICE,"brd[%s]: cb is null \n", __func__);
	return 1;
    }

    //find primary rb 
    rb_index = session.mapping_lbg_to_rb[0][lbg_index];
    rb = cb->rbh.remote_block[rb_index];

    DEBUG_LOG(LOG_NOTICE, "[%s] buf:%p size:%ld, offset:%ld block_index:%d lbg_index:%d rb_index:%d \n",__func__, buf, len, (long)offset, block_index, lbg_index, rb_index);

    //TODO : eviction migration
    /*
    // check if READONLY
    if(rb->rb_state >= READONLY){
    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: this rb is READONLY. MIGRATION on progress\n",__func__);
try_again:
ret = put_sending_index(tmp);
if(ret){
syslog(LOG_NOTICE,"brd[%s]: Fail to put sending index. retry \n", __func__);
goto try_again;
}
return 0;
}
     */
    for(j=0; j<NUM_REPLICA; j++){
retry:
	ctx[j] = get_ctx_freepool();
	if (!ctx[j]){
	    //syslog(LOG_NOTICE,"rdmabox[%s]: try to get ctx[%d] \n",__func__,j);
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: try to get ctx[%d] \n",__func__,j);
	    goto retry;
	}
	//TODO : in_flight
	//atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	ctx[j]->replica_index = j;

	if(j==0){
	    //#ifndef DYNAMIC_MR_REG
	    if( len < 950272 ){ // 928KB
		DEBUG_LOG(LOG_NOTICE,"use Pre-MR\n");
retry_mr:
		get_mr_freepool(session.mr_freepool, &mr);
		if(!mr){
		    //syslog(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
		    DEBUG_LOG(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
		    goto retry_mr;
		}
		ctx[j]->mr = mr;
		ctx[j]->isDynamic = 0;
		memcpy(mr->rdma_buf,buf,len);
		ctx[j]->reqlist[0] = req;
		ctx[j]->numreq = 1;
		//#else
	    }else{ // len >= 928KB
		DEBUG_LOG(LOG_NOTICE,"use Dynamic-MR\n");

		ctx[j]->dynamic_mr.rdma_buf = buf;
		ctx[j]->dynamic_mr.rdma_mr = ibv_reg_mr(session.pd, buf, len,
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
		if (!ctx[j]->dynamic_mr.rdma_mr) {
		    syslog(LOG_NOTICE,stderr, "rdma_buf reg_mr failed\n");
		    ret = errno;
		}
		ctx[j]->isDynamic = 1;
		ctx[j]->reqlist[0] = req;
		ctx[j]->numreq = 1;
		//#endif
	    }//else
	    ctx[j]->len =  len;
	}else{
	    ctx[j]->prime_ctx = ctx[0];
	}

	ctx[j]->rdma_sq_wr.sg_list = &ctx[j]->rdma_sgl;
	ctx[j]->rdma_sq_wr.num_sge = 1;
    }//replica forloop

// TODO : disk backup
/*
#ifdef DISKBACKUP
disk_write(ctx[0], tmp->cloned_bio, tmp->len*VALET_PAGE_SIZE, tmp->start_index);
#endif
 */

// send out
for(j=0; j<NUM_REPLICA; j++){
    ret =  rdma_write(&session, replica_cb_list[j], block_index, offset, j, ctx[j]);
    if (ret)
	return ret;
}

return 0;
}

int remote_sender_write(struct session *sess, struct local_page_list *tmp)
{
    int retval = 0;
    int i,j,k;
    int ret;
    int block_index;
    int lbg_index;
    struct rdma_ctx *ctx[NUM_REPLICA];
    struct rdma_mr *mr;
    struct krping_cb *cb;
    struct remote_block *rb;
    int rb_index;
    struct krping_cb *replica_cb_list[NUM_REPLICA];
    unsigned long cur_offset;
    unsigned long offset;

    block_index = tmp->start_index;
    lbg_index = tmp->start_index / LOCAL_BLOCKS_PER_REMOTE_BLOCK;
    offset = tmp->start_offset;

    //find primary cb
    cb = get_cb(&session, lbg_index, replica_cb_list);
    if(!cb){
	syslog(LOG_NOTICE,"[%s]: cb is null \n", __func__);
	return 1;
    }

    //find primary rb 
    rb_index = session.mapping_lbg_to_rb[0][lbg_index];
    rb = cb->rbh.remote_block[rb_index];
    //DEBUG_LOG(LOG_NOTICE, "[%s] block_index:%d lbg_index:%d, rb_index:%d\n",__func__, block_index, lbg_index, rb_index);

retry_ctx:
    ctx[0] = get_ctx_freepool();
    if (!ctx[0]){
	//syslog(LOG_NOTICE,"rdmabox[%s]: try to get ctx[0] \n",__func__);
	DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: try to get ctx[0] \n",__func__);
	goto retry_ctx;
    }
retry_mr:
    get_mr_freepool(session.mr_freepool, &mr);
    if(!mr){
	//syslog(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
	DEBUG_LOG(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
	goto retry_mr;
    }

    ctx[0]->mr = mr;
    ctx[0]->isDynamic = 0;
    ctx[0]->numreq = 0;
    ctx[0]->reqlist[0] = NULL;
    ctx[0]->sending_item = tmp;
    ctx[0]->len = 0;
    atomic_set(ctx[0]->sending_item->ref_count, 0);
    atomic_set(ctx[0]->ref_count, 0);

    cur_offset = 0;
    for(i=0; i<tmp->len; i++){
	// merge into rdmabuf
	if(i==0)
	    memcpy(mr->rdma_buf, tmp->batch_list[i]->page + tmp->start_offset, tmp->size_list[i]);
	else
	    memcpy(mr->rdma_buf + cur_offset, tmp->batch_list[i]->page, tmp->size_list[i]);
	cur_offset = cur_offset + tmp->size_list[i];
	ctx[0]->len = ctx[0]->len + tmp->size_list[i];
	//if(i>0)
	//    syslog(LOG_NOTICE,"rdmabox[%s]: merge. block_index:%d batch_list[%d] \n", __func__,block_index,i);
    }

    // process replica
    for(j=0; j<NUM_REPLICA; j++){
	if(j>0){
retry_ctx2:
	    ctx[j] = get_ctx_freepool();
	    if (!ctx[j]){
		//syslog(LOG_NOTICE,"rdmabox[%s]: try to get ctx[%d] \n",__func__,j);
		DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: try to get ctx[%d] \n",__func__,j);
		goto retry_ctx2;
	    }
	    ctx[j]->mr = ctx[0]->mr;
	    ctx[j]->len = ctx[0]->len;
	}//j>0
	//atomic_set(&ctx[j]->in_flight, CTX_W_IN_FLIGHT);
	ctx[j]->replica_index = j;
	ctx[j]->isDynamic = 0;
	ctx[j]->prime_ctx = ctx[0];

	ctx[j]->rdma_sq_wr.sg_list = &ctx[j]->rdma_sgl;
	ctx[j]->rdma_sq_wr.num_sge = 1;
    }//replica forloop

    // send out
    for(j=0; j<NUM_REPLICA; j++){
	ret =  rdma_write(&session, replica_cb_list[j], block_index, offset, j, ctx[j]);
	if (ret){
	    return ret;
	}
    }

    return 0;

}
/*
   void notify_sending_list_full()
   {
   DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: wake up thread \n", __func__);
   sem_post(&session.sending_list_sem);
   }
 */

void remote_sender_full_fn(void * arg)
{
    struct session *sess = arg;
    struct local_page_list *tmp_first=NULL;
    int err;
    int i;
    int must_go = 0;
    int tmp_index;
    int block_index, block_index_first;
    int lbg_index, lbg_index_first;

    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: start thread \n", __func__);

    while(true){
	// take the item to send from sending list
	//wait_for_item:
	struct local_page_list *tmp;
	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: sleep \n", __func__);
	//sem_wait(&sess->sending_list_sem);
	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: wake up \n", __func__);
	err = get_sending_index(&tmp);
	if(err){
	    if(tmp_first){
		//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: No items more. Send tmp_first \n", __func__);
		tmp = NULL;
		goto send;
	    }
	    //usleep(100000);
	    //goto wait_for_item;
	    continue;
	}

	block_index = tmp->start_index;
	lbg_index = tmp->start_index / LOCAL_BLOCKS_PER_REMOTE_BLOCK;

	if(!tmp_first)
	{
reload:
	    tmp_first=tmp;
	    block_index_first = block_index;
	    lbg_index_first = lbg_index;
	    tmp_index = tmp_first->start_index;
	    i = tmp_first->len;

	    if(must_go){
		tmp = NULL;
		goto send;
	    }else{
		continue;
	    }
	}

	//TODO : two chunk
	if (tmp_first && lbg_index_first != lbg_index) {
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: two chunk. block_index_first:%d, block_index:%d lbg_index_first:%d, lbg_index:%d \n", __func__,block_index_first,block_index,lbg_index_first, lbg_index);
	    goto send;
	}

	// check if there is contiguous item
	// check if it fits into one item
	if( (tmp->start_index == tmp_index + 1) &&
		(tmp_first->len + tmp->len <= RDMA_WR_BUF_LEN) )
	{
	    // merge items
	    tmp_index = tmp->start_index;
	    tmp_first->batch_list[i] = tmp->batch_list[0];
	    tmp_first->size_list[i] = tmp->size_list[0];
	    tmp_first->len = tmp_first->len + tmp->len;
	    i++;

	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: merge. num_batch:%d index tmp_first:%d tmp:%d lbg_index_first:%d lbg_index:%d\n", __func__,tmp_first->len,tmp_first->start_index, tmp->start_index, lbg_index_first, lbg_index);
	    // release unused item
	    put_free_item(tmp);
	    continue;
	}

send:
	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: now send. remote write index:%lu num_batch:%d\n", __func__,tmp_first->start_index,tmp_first->len);
	// write remote
	err = remote_sender_write(sess, tmp_first);
	if(err){
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: remote write fail\n", __func__);
	    // TODO : fail exception handling. put sending list again ?
try_again:
	    err = put_sending_index(tmp_first);
	    if(err){
		DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: Fail to put sending index. retry \n", __func__);
		//usleep(1000);
		goto try_again;
	    }
	}

	if(!tmp){
	    must_go = 0;
	    //goto wait_for_item;
	    continue;
	}else{
	    //must_go = 1;
	    goto reload;
	}

    }//while 
}

// for direct call
int remote_sender_fn(struct session *sess)
{
    struct local_page_list *tmp_first=NULL;
    int err;
    int i;
    int must_go = 0;
    int tmp_index;

    while(true){
	// take the item to send from sending list
	struct local_page_list *tmp;
	err = get_sending_index(&tmp);
	if(err){
	    if(tmp_first){
		//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: No items more. Send tmp_first \n", __func__);
		tmp = NULL;
		goto send;
	    }
	    return 0;
	}

	if(!tmp_first)
	{
	    tmp_first=tmp;
	    tmp_index = tmp->start_index;

	    if(must_go){
		tmp = NULL;
		goto send;
	    }else{
		continue;
	    }
	}
	// check if there is contiguous item
	// check if it fits into one item
	// TODO : fix batch
	if( (tmp->start_index == tmp_index + 1) &&
		(tmp_first->len + tmp->len <= RDMA_WR_BUF_LEN) )
	{
	    // merge items
	    tmp_index = tmp->start_index;
	    int j = tmp_first->len;
	    for(i=0; i < tmp->len ;i++){
		tmp_first->batch_list[j] = tmp->batch_list[i];
		j++;
	    }
	    tmp_first->len = tmp_first->len + tmp->len;

	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: merge. index tmp_first:%d tmp:%d \n", __func__,tmp_first->start_index, tmp->start_index);
	    // release unused item
	    put_free_item(tmp);
	    continue;
	}
send:
	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: remote write index:%lu\n", __func__,tmp_first->start_index);
	// write remote
	err = remote_sender_write(sess, tmp_first);
	if(err){
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: remote write fail\n", __func__);
	    // TODO : fail exception handling. put sending list again ?
try_again:
	    err = put_sending_index(tmp_first);
	    if(err){
		DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: Fail to put sending index. retry \n", __func__);
		//usleep(1000);
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

int batch_write()
{
    struct urdmabox_req *tmp_first=NULL;
    int err;
    int i,j;
    int must_go = 0;
    int lbg_index, lbg_index_first;
    unsigned long cur_offset;
    unsigned long offset, offset_first;
    int block_index, block_index_first;
    struct rdma_ctx *ctx[NUM_REPLICA];
    struct rdma_mr *mr;
    struct krping_cb *cb;
    struct remote_block *rb;
    int rb_index;
    struct krping_cb *replica_cb_list[NUM_REPLICA];
    struct krping_cb *replica_cb_list_first[NUM_REPLICA];

    while(true){
	struct urdmabox_req *tmp;
	err = get_wr_req_item(&tmp);
	//DEBUG_LOG(LOG_NOTICE, "write remote buf:%p size:%ld, offset:%ld block_index:%d\n", tmp->buf, tmp->size, (long)tmp->offset, tmp->block_index);
	if(err){
	    if(tmp_first){
		tmp = NULL;
		goto send;
	    }
	    return 0;
	}

	block_index = tmp->block_index;
	lbg_index = tmp->block_index / LOCAL_BLOCKS_PER_REMOTE_BLOCK;

	//find primary cb
	cb = get_cb(&session, lbg_index, replica_cb_list);
	if(!cb){
	    syslog(LOG_NOTICE,"[%s]: cb is null \n", __func__);
	    return 1;
	}

	//find primary rb 
	rb_index = session.mapping_lbg_to_rb[0][lbg_index];
	rb = cb->rbh.remote_block[rb_index];
	//DEBUG_LOG(LOG_NOTICE, "block_index:%d lbg_index:%d, rb_index:%d\n", block_index, lbg_index, rb_index);

	if(!tmp_first)
	{
reload:
	    tmp_first=tmp;
	    cur_offset = 0;
	    offset_first = tmp->offset;
	    lbg_index_first = lbg_index;
	    for (i=0; i<NUM_REPLICA; i++){
		replica_cb_list_first[i] = replica_cb_list[i];
	    }
	    block_index_first = block_index;
	    // get prime ctx and mr
retry:
	    ctx[0] = get_ctx_freepool();
	    if (!ctx[0]){
		//syslog(LOG_NOTICE,"rdmabox[%s]: try to get ctx[0] \n",__func__);
		DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: try to get ctx[0] \n",__func__);
		goto retry;
	    }
retry_mr:
	    get_mr_freepool(session.mr_freepool, &mr);
	    if(!mr){
		//syslog(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
		DEBUG_LOG(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
		goto retry_mr;
	    }

	    ctx[0]->mr = mr;
	    ctx[0]->isDynamic = 0;
	    ctx[0]->numreq = 0;
	    atomic_set(ctx[0]->ref_count,0);
	    cur_offset = tmp_first->size;
	    ctx[0]->reqlist[ctx[0]->numreq++] = tmp_first;
	    ctx[0]->len = tmp_first->size;

#ifndef DYNAMIC_MR_REG
	    // data copy into pre-MR
	    memcpy(ctx[0]->mr->rdma_buf,tmp_first->buf,tmp_first->size);
#endif

	    if(must_go){
		tmp = NULL;
		goto send;
	    }else{
		continue;
	    }
	}

	if (tmp_first && lbg_index_first != lbg_index) {
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: two chunk. block_index_first:%d, block_index:%d lbg_index_first:%d, lbg_index:%d \n", __func__,block_index_first,block_index,lbg_index_first, lbg_index);
	    goto send;
	}

	// check if it is next contiguous item
	// check if it fits into one item
	// TODO : fix batch
	if( (block_index == block_index_first + 1) &&
		(cur_offset + tmp->size <= RDMA_BUF_SIZE) )
	{
	    // merge items
#ifndef DYNAMIC_MR_REG
	    memcpy(ctx[0]->mr->rdma_buf + ctx[0]->len,tmp->buf,tmp->size);
#endif
	    cur_offset = cur_offset + tmp->size;
	    ctx[0]->reqlist[ctx[0]->numreq++] = tmp;
	    ctx[0]->len = ctx[0]->len + tmp->size;
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: merge. block_index_first:%d, block_index:%d numreq:%d \n", __func__,block_index_first,block_index,ctx[0]->numreq);
	    continue;
	}

send:
	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: remote write. numreq:%d block_index_first:%d block_index:%d \n", __func__,ctx[0]->numreq,block_index_first, block_index);
	err = remote_write(&session, ctx, replica_cb_list_first, block_index_first, offset_first);
	if(err){
	    syslog(LOG_NOTICE,"rdmabox[%s]: remote write fail\n", __func__);
	    return 1;
	}
	if(!tmp){
	    must_go = 0;
	    tmp_first = NULL;
	    continue;
	}else{
	    must_go = 1;
	    goto reload;
	}

    }//while

    return 0;
}

void batch_read()
{
    struct urdmabox_req *tmp_first=NULL;
    int err;
    int i,j;
    int must_go = 0;
    int lbg_index, lbg_index_first;
    unsigned long cur_offset;
    unsigned long offset, offset_first;
    int block_index, block_index_first;
    struct rdma_mr *mr;
    struct krping_cb *cb;
    struct remote_block *rb, *rb_first;
    int rb_index, rb_index_first;
    int cb_index;
    struct rdma_ctx *ctx;
    int tmp_index;
    unsigned long tmp_offset;

    while(true){
	struct urdmabox_req *tmp;
	err = get_rd_req_item(&tmp);
	if(err){
	    if(tmp_first){
		tmp = NULL;
		goto send;
	    }
	    return 0;
	}

	must_go = 1;

	block_index = tmp->block_index;
	lbg_index = tmp->block_index / LOCAL_BLOCKS_PER_REMOTE_BLOCK;

	//DEBUG_LOG(LOG_NOTICE, "[%s] get_item buf:%p size:%ld, offset:%ld block_index:%d lbg_index:%d\n",__func__, tmp->buf, tmp->size, (long)tmp->offset, tmp->block_index,lbg_index);

	cb_index = atomic_read(session.cb_index_map[0] + lbg_index);
	if (cb_index == NO_CB_MAPPED){
	    // TODO : backup read(replica or disk)
	    syslog(LOG_NOTICE, "NO_CB_MAPPED block_index:%d lbg_inde:%d\n",block_index,lbg_index);
	    return;
	}
	cb = session.cb_list[cb_index];

	//find primary rb 
	rb_index = session.mapping_lbg_to_rb[0][lbg_index];
	rb = cb->rbh.remote_block[rb_index];


	if(!tmp_first)
	{
reload:
	    tmp_first=tmp;
	    cur_offset = 0;
	    offset_first = tmp->offset;
	    rb_index_first = rb_index;
	    rb_first = rb;
	    lbg_index_first = lbg_index;
	    block_index_first = block_index;
	    tmp_index = tmp_first->block_index;
	    tmp_offset = tmp_first->offset;
	    // get prime ctx and mr
retry:
	    ctx = get_ctx_freepool();
	    if (!ctx){
		//syslog(LOG_NOTICE,"rdmabox[%s]: try to get ctx[0] \n",__func__);
		DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: try to get ctx[0] \n",__func__);
		goto retry;
	    }
retry_mr:
	    get_mr_freepool(session.mr_freepool, &mr);
	    if(!mr){
		//syslog(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
		DEBUG_LOG(LOG_NOTICE,"[%s] retry_mr get_mr_freepool\n",__func__);
		goto retry_mr;
	    }

	    ctx->mr = mr;
	    ctx->isDynamic = 0;
	    ctx->numreq = 0;
	    //atomic_set(ctx->ref_count,0);
	    ctx->req_buf = tmp_first->buf;
	    ctx->len = tmp_first->size;
	    cur_offset = tmp_first->size;
	    ctx->reqlist[ctx->numreq++] = tmp_first;

	    if(must_go){
		tmp = NULL;
		goto send;
	    }else{
		continue;
	    }
	}

	if (tmp_first && lbg_index_first != lbg_index) {
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: two chunk. block_index_first:%d, block_index:%d lbg_index_first:%d, lbg_index:%d \n", __func__,block_index_first,block_index,lbg_index_first, lbg_index);
	    goto send;
	}

	// check if it is next contiguous item
	// check if it fits into one item
	// TODO : fix batch
	if( ( (block_index == tmp_index + 1) || ((block_index == tmp_index) && (tmp->offset == tmp_offset + FUSE_BLOCK_SIZE) ))
	     && (cur_offset + tmp->size <= RDMA_BUF_SIZE) )
	{
	    // merge items
	    tmp_index = tmp->block_index;
	    cur_offset = cur_offset + tmp->size;
	    ctx->reqlist[ctx->numreq++] = tmp;
	    ctx->len = ctx->len + tmp->size;
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: merge. block_index_first:%d, block_index:%d numreq:%d \n", __func__,block_index_first,block_index,ctx->numreq);
	    continue;
	}

send:
	DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: ctxid:%d remote read. numreq:%d block_index_first:%d block_index:%d \n", __func__,ctx->id,ctx->numreq,block_index_first, block_index);

	ctx->rdma_sq_wr.sg_list = &ctx->rdma_sgl;
	ctx->rdma_sq_wr.num_sge = 1;

	err = rdma_read(&session, cb, rb_index_first, rb_first, offset_first, block_index_first, ctx);
	if(err){
	    syslog(LOG_NOTICE,"rdmabox[%s]: remote write fail\n", __func__);
	    return 1;
	}
	if(!tmp){
	    must_go = 0;
	    tmp_first = NULL;
	    continue;
	}else{
	    must_go = 1;
	    goto reload;
	}

    }//while

    return 0;
}


void post_setup_connection(struct session *session, struct krping_cb *tmp_cb, int cb_index)
{
    //char name[2];

    // single mr
    if(!session->mr_freepool->init_done)
	mr_freepool_post_init(session, session->mr_freepool, RDMA_BUF_SIZE);

    // ctx + mr
    //if(!session->rdma_write_freepool->init_done)
    //rdma_freepool_post_init(session, tmp_cb, session->rdma_write_freepool, WR_MR_SIZE, 1);
    //if(!session->rdma_read_freepool->init_done)
    //rdma_freepool_post_init(session, tmp_cb, session->rdma_read_freepool, RD_MR_SIZE, 0);

    //memset(name, '\0', 2);
    //name[0] = (char)((cb_index/26) + 97);
    //TODO : enable migration_handler
    //pthread_create(&tmp_cb->rbh.mig_thread, NULL, migration_handler, tmp_cb);
}

void remote_block_init(struct krping_cb *cb)
{
    int i = 0;
    struct remote_block *rb;
    int select_idx = cb->recv_buf.size_gb;
    int replica_index = cb->recv_buf.replica_index;
    int rb_index = cb->recv_buf.rb_index;
    struct session *session = cb->sess;

    if (cb->recv_buf.rkey) {
	cb->rbh.remote_block[rb_index]->replica_index = replica_index;
	cb->rbh.remote_block[rb_index]->remote_rkey = ntohl(cb->recv_buf.rkey);
	cb->rbh.remote_block[rb_index]->remote_addr = ntohll(cb->recv_buf.buf);
	cb->rbh.remote_block[rb_index]->bitmap_g = calloc(MAX_RB_NUM, 1);;
	session->mapping_lbg_to_rb[replica_index][select_idx] = rb_index;
	cb->rbh.rb_idx_map[rb_index] = select_idx;

	cb->rbh.block_size_gb += 1;
	cb->rbh.c_state = RBH_READY;
	atomic_set(cb->rbh.remote_mapped + rb_index, RB_MAPPED);
	atomic_set(session->cb_index_map[replica_index] + (select_idx), cb->cb_index);

	rb = cb->rbh.remote_block[rb_index];
	rb->rb_state = ACTIVE; 

	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: replica_index:%d (rb[%d] on node[%d]) is mapped to local block group[%d]\n",__func__,replica_index,rb_index,cb->cb_index,select_idx);
    }
}

struct krping_cb* get_cb(struct session *session, int lbg_index, struct krping_cb **replica_cb_list)
{
    int i;
    int ret;
    int cb_index;

    pthread_spin_lock(&session->cb_lock);
    cb_index = atomic_read(session->cb_index_map[0] + lbg_index);
    if (cb_index == NO_CB_MAPPED){
	ret = remote_block_map(session, lbg_index, replica_cb_list);
	if(ret != NUM_REPLICA){
	    pthread_spin_unlock(&session->cb_lock);
	    return NULL;
	}
    }else{
	replica_cb_list[0] = session->cb_list[cb_index];
	for(i=1; i<NUM_REPLICA; i++){
	    cb_index = atomic_read(session->cb_index_map[i] + lbg_index);
	    replica_cb_list[i] = session->cb_list[cb_index];
	}
    }
    pthread_spin_unlock(&session->cb_lock);

    return replica_cb_list[0];
}

int remote_block_map(struct session *session, int select_idx, struct krping_cb **replica_cb_list)
{
    struct krping_cb *tmp_cb;
    int cb_index;
    int replica_list[NUM_REPLICA];
    int rst=0;
    int i;
    rst = find_the_best_node(session,replica_list,NUM_REPLICA,0);
    if(rst < NUM_REPLICA){
	syslog(LOG_NOTICE,"rdmabox[%s]: fail to find_the_best_node num_found:%d\n",__func__,rst);
	return rst;
    }

    rst=0;
    for(i=0; i<NUM_REPLICA; i++){
	cb_index = replica_list[i];
	tmp_cb = session->cb_list[cb_index];
	if (session->cb_state_list[cb_index] == CB_CONNECTED){ 
	    post_setup_connection(session, tmp_cb, cb_index);
	}

	tmp_cb->send_buf.type = REQ_MR_MAP;
	tmp_cb->send_buf.size_gb = select_idx; 
	tmp_cb->send_buf.replica_index = i; //use this for marking for replica index
	post_send(tmp_cb);
	sem_wait(&tmp_cb->sem);
	if (tmp_cb->state != RECV_MR_MAP) {
	    syslog(LOG_NOTICE,stderr, "wait for RECV_MR_MAP state %d\n", tmp_cb->state);
	    return -1;
	}

	replica_cb_list[i] = tmp_cb;
	rst++;
    }//replica loop

    return rst;
}

int find_the_best_node(struct session *session, int *replica_list, int num_replica, int isMig){

    int i, j, k;
    int num_possible_servers=0;
    int num_available_servers=0;
    struct krping_cb *tmp_cb;
    int candidate[session->cb_num];
    unsigned int select_history[session->cb_num];
    unsigned int cb_index;

    int free_mem[SERVER_SELECT_NUM];
    int free_mem_sorted[SERVER_SELECT_NUM]; 
    int tmp_int;
    unsigned long tmp_ul;
    // initialization
    for (i = 0; i < SERVER_SELECT_NUM; i++){
	free_mem[i] = -1;
	free_mem_sorted[i] = session->cb_num;
    }
    for (i=0; i < session->cb_num ;i++){
	candidate[i] = -1;
	select_history[i] = 0;
    }

    // find out available servers
    for (i=0; i<session->cb_num;i++){
	if (session->cb_state_list[i] < CB_MIGRATING) {
	    candidate[num_possible_servers]=i;
	    num_possible_servers++;
	}
    }

    // select random nodes
    for (i=0; i < num_possible_servers ;i++){

	if(num_possible_servers <= SERVER_SELECT_NUM){
	    cb_index = candidate[i];
	}else{
	    srand(time(NULL));   // Initialization, should only be called once.
	    int k = rand();
	    //get_random_bytes(&k, sizeof(unsigned int));
	    k %= num_possible_servers;
	    while (select_history[k] == 1) {
		k += 1;	
		k %= num_possible_servers;
	    }
	    select_history[k] = 1;
	    cb_index = candidate[k];
	}

	tmp_cb = session->cb_list[cb_index];
	if (session->cb_state_list[cb_index] > CB_IDLE) {
	    tmp_cb->send_buf.type = REQ_QUERY;
	    post_send(tmp_cb);
	    sem_wait(&tmp_cb->sem);
	    if (tmp_cb->state != RECV_QUERY_REPL) {
		syslog(LOG_NOTICE,stderr, "wait for CONNECTED state %d\n", tmp_cb->state);
		return -1;
	    }
	}else { // cb_idle
	    control_block_init(tmp_cb, session);
	    connect_remote(tmp_cb, session);
	    sem_wait(&tmp_cb->sem);
	    if (tmp_cb->state != RECV_QUERY_REPL) {
		syslog(LOG_NOTICE,stderr, "wait for CONNECTED state %d\n", tmp_cb->state);
		return -1;
	    }
	    session->cb_state_list[cb_index] = CB_CONNECTED; //add cb_connected
	}

	if(tmp_cb->rbh.target_size_gb == 0){
	    continue;
	}

	free_mem[num_available_servers] = tmp_cb->rbh.target_size_gb;
	free_mem_sorted[num_available_servers] = cb_index;
	num_available_servers++;
	if(num_available_servers == SERVER_SELECT_NUM)
	    break;
    }//for

    // remote nodes have not enough free_mem
    if(num_available_servers < num_replica)
	return 0;

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
    j = 0;
    for(i=0; i<num_replica; i++){
	replica_list[j] = free_mem_sorted[i];
	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: selected node : replica[%d]=%d \n",__func__,j,replica_list[j]);
	j++;
    }

    return j;
}

int put_ctx_freepool(struct rdma_ctx *tmp)
{
    if (lfqueue_enq(session.ctx_freepool, tmp) < 0){
	syslog(LOG_NOTICE,"mempool[%s]: Fail to put wr req item \n",__func__);
	return 1;
    }
    //DEBUG_LOG(LOG_NOTICE,"[%s]:put ctxid:%d count=%u \n", __func__,tmp->id,lfqueue_size(session.ctx_freepool));
    return 0;
}

struct rdma_ctx * get_ctx_freepool()
{
    struct rdma_ctx *tmp;

    if (lfqueue_size(session.ctx_freepool) == 0){
	return NULL;
    }

    tmp = (struct urdmabox_req*)lfqueue_deq_must(session.ctx_freepool);
    if(!tmp){
	return NULL;
    }
    //DEBUG_LOG(LOG_NOTICE,"[%s]:get ctxid:%d count=%u \n", __func__,tmp->id,lfqueue_size(session.ctx_freepool));

    return tmp;
}


/*
//TODO: change name mr_elements -> elements
void put_ctx_freepool(rdma_freepool_t *pool, struct rdma_ctx *ctx_element)
{       
pthread_spin_lock(&pool->rdma_freepool_lock);
if(ctx_element != NULL){
pool->mr_elements[pool->curr_nr_mr++] = ctx_element;
}
pthread_spin_unlock(&pool->rdma_freepool_lock);
}

void get_ctx_freepool(rdma_freepool_t *pool, struct rdma_ctx **ctx_element)
{
if (pool->curr_nr_mr == 0){
 *ctx_element = NULL;
 return;
 }
 pthread_spin_lock(&pool->rdma_freepool_lock);
 if (pool->curr_nr_mr > 0) {
 *ctx_element = pool->mr_elements[--pool->curr_nr_mr];
 }else{
 *ctx_element = NULL;
 }
 pthread_spin_unlock(&pool->rdma_freepool_lock);

 return;
 }
 */

void put_mr_freepool(rdma_freepool_t *pool, struct rdma_mr *mr_element)
{       
    pthread_spin_lock(&pool->rdma_freepool_lock);
    if(mr_element != NULL){
	pool->mr_elements[pool->curr_nr_mr++] = mr_element;
    }
    pthread_spin_unlock(&pool->rdma_freepool_lock);
}
void get_mr_freepool(rdma_freepool_t *pool, struct rdma_mr **mr_element)
{

    if (pool->curr_nr_mr == 0){
	*mr_element = NULL;
	return;
    }
    pthread_spin_lock(&pool->rdma_freepool_lock);
    if (pool->curr_nr_mr > 0) {
	*mr_element = pool->mr_elements[--pool->curr_nr_mr];
    }else{
	*mr_element = NULL;
    }
    pthread_spin_unlock(&pool->rdma_freepool_lock);

    return;
}

int ctx_freepool_create(struct session *session, struct rdma_freepool_s **cpupool, unsigned long cap_nr)
{
    int i;
    lfqueue_t *pool;
    struct rdma_ctx *ctx;

    pool = malloc(sizeof(lfqueue_t));
    if (lfqueue_init(pool) == -1){
	syslog(LOG_NOTICE,"session_create - fail to malloc wr_req_q \n");
	return -1;
    }
    *cpupool = pool;

    for(i=0; i<cap_nr; i++){
	ctx = (struct rdma_ctx *)malloc(sizeof(struct rdma_ctx));
	ctx->id = i;
	ctx->ref_count = (struct atomic_t *)malloc(sizeof(struct atomic_t));
	atomic_init(ctx->ref_count);
	atomic_set(ctx->ref_count, 0);
	ctx->sending_item = NULL;
	if (!ctx) {
	    syslog(LOG_NOTICE,"rdmabox[%s]: ctx malloc failed\n",__func__);
	    free(ctx);
	    return 1;
	}
	put_ctx_freepool(ctx);
    }

    return 0;

    /*
       int i;
       struct rdma_freepool_s *pool;
       struct rdma_ctx *ctx;


       pool = (struct rdma_freepool_s *)malloc(sizeof(rdma_freepool_t));
       if (!pool){
       syslog(LOG_NOTICE,"mempool[%s]: Fail to create pool  \n",__func__);
       return 1;
       }
     *cpupool = pool;
     pthread_spin_init(&pool->rdma_freepool_lock,0);
     pool->init_done = 0;

     pool->mr_elements = malloc(cap_nr * sizeof(void *));
     if (!pool->mr_elements) {
     syslog(LOG_NOTICE,"mempool[%s]: Fail to create pool elements  \n",__func__);
     free(pool);
     return 1;
     }
     pool->cap_nr_mr = cap_nr;
     pool->curr_nr_mr = 0;

     while (pool->curr_nr_mr < pool->cap_nr_mr) {
     ctx = (struct rdma_ctx *)malloc(sizeof(struct rdma_ctx));
     ctx->ref_count = (struct atomic_t *)malloc(sizeof(struct atomic_t));
     atomic_init(ctx->ref_count);
     atomic_set(ctx->ref_count, 0);
     ctx->sending_item = NULL;
     if (!ctx) {
     syslog(LOG_NOTICE,"rdmabox[%s]: ctx malloc failed\n",__func__);
     free(ctx);
     return 1;
     }
     put_ctx_freepool(ctx);
     }

     pool->init_done = 1;

     syslog(LOG_NOTICE,"mempool[%s]: freepool create done. curr_nr=%ld \n",__func__,pool->curr_nr_mr);

     return 0;
     */
}

int rdma_freepool_create(struct session *session, struct rdma_freepool_s **cpupool, unsigned long cap_nr_mr, int buf_size)
{
    int i;
    struct rdma_mr *mr;
    struct rdma_freepool_s *pool;

    pool = (struct rdma_freepool_s *)malloc(sizeof(rdma_freepool_t));
    if (!pool){
	syslog(LOG_NOTICE,"mempool[%s]: Fail to create pool  \n",__func__);
	return 1;
    }
    *cpupool = pool;
    pthread_spin_init(&pool->rdma_freepool_lock,0);
    pool->init_done = 0;

    pool->mr_elements = malloc(cap_nr_mr * sizeof(void *));
    if (!pool->mr_elements) {
	syslog(LOG_NOTICE,"mempool[%s]: Fail to create pool elements  \n",__func__);
	free(pool);
	return 1;
    }
    pool->cap_nr_mr = cap_nr_mr;
    pool->curr_nr_mr = 0;

    while (pool->curr_nr_mr < pool->cap_nr_mr) {
	mr = (struct rdma_mr *)malloc(sizeof(struct rdma_mr));
	mr->rdma_buf = malloc(buf_size);
	if (!mr->rdma_buf) {
	    syslog(LOG_NOTICE,"rdmabox[%s]: rdma_buf malloc failed\n",__func__);
	    return 1;
	}
	/* //should be moved to post_init. No pd here
	   mr->rdma_mr = ibv_reg_mr(session->pd, mr->rdma_buf, buf_size,
	   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
	   if (!mr->rdma_mr) {
	   syslog(LOG_NOTICE,"rdmabox[%s]: rdma_mr ibv_reg_mr failed\n",__func__);
	   free(mr->rdma_buf);
	   return 1;
	   }
	 */
	put_mr_freepool(pool,mr);
    }

    //pool->init_done = 1;
    syslog(LOG_NOTICE,"mempool[%s]: freepool create done. curr_nr=%ld \n",__func__,pool->curr_nr_mr);

    return 0;
}

void *cm_thread(void *arg)
{
    struct krping_cb *cb = arg;
    struct rdma_cm_event *event;
    int ret;

    while (1) {
	ret = rdma_get_cm_event(cb->cm_channel, &event);
	if (ret) {
	    syslog(LOG_NOTICE,stderr, "rdma_get_cm_event err %d\n", ret);
	    exit(ret);
	}
	struct rdma_cm_event event_copy;

	memcpy(&event_copy, event, sizeof(*event));
	rdma_ack_cm_event(event);

	ret = krping_cma_event_handler(&event_copy);
	if (ret)
	    exit(ret);
    }
}

int krping_cma_event_handler(struct rdma_cm_event *event)
{
    int ret;
    struct conn_private_data *pdata;
    struct rdma_cm_id *cma_id = event->id;
    struct krping_cb *cb = cma_id->context; // listener_cb
    struct portal *portal;

    switch (event->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
	    DEBUG_LOG(LOG_NOTICE,"[%s]: RDMA_CM_EVENT_ADDR_RESOLVED \n",__func__);
	    cb->state = ADDR_RESOLVED;
	    ret = rdma_resolve_route(cma_id, 2000);
	    if (ret) {
		syslog(LOG_NOTICE,"rdma_resolve_route error %d\n", 
			ret);
		sem_post(&cb->sem);
	    }
	    break;

	case RDMA_CM_EVENT_ROUTE_RESOLVED:
	    DEBUG_LOG(LOG_NOTICE,"[%s]: RDMA_CM_EVENT_ROUTE_RESOLVED  \n",__func__);
	    cb->state = ROUTE_RESOLVED;
	    sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_CONNECT_REQUEST:
	    cb->state = CONNECT_REQUEST;
	    cb->child_cm_id = cma_id;
	    portal = (struct portal *)event->param.conn.private_data;
	    DEBUG_LOG(LOG_NOTICE,"[%s]: RDMA_CM_EVENT_CONNECT_REQUEST child cma %p\n",__func__, cb->child_cm_id);
	    DEBUG_LOG(LOG_NOTICE,"[%s]: cm_id:%p child_cm_id:%p \n",__func__, cb->cm_id, cb->child_cm_id);
	    DEBUG_LOG(LOG_NOTICE,"[%s]: incoming src addr : %s %d\n",__func__, portal->addr, portal->port);
	    setup_connection(cb->sess, portal, cb);
	    //sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_ESTABLISHED:
	    DEBUG_LOG(LOG_NOTICE,"[%s]: ESTABLISHED\n",__func__);
	    cb->state = CONNECTED;
	    sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_ADDR_ERROR:
	    syslog(LOG_NOTICE,"[%s]: RDMA_CM_EVENT_ADDR_ERROR \n",__func__);
	    syslog(LOG_NOTICE,"cma event %d, error %d\n", event->event,
		    event->status);
	    cb->state = ERROR;
	    sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_ROUTE_ERROR:
	    syslog(LOG_NOTICE,"[%s]: RDMA_CM_EVENT_ROUTE_ERROR \n",__func__);
	    syslog(LOG_NOTICE,"cma event %d, error %d\n", event->event,
		    event->status);
	    cb->state = ERROR;
	    sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_CONNECT_ERROR:
	    syslog(LOG_NOTICE,"[%s]: RDMA_CM_EVENT_CONNECT_ERROR \n",__func__);
	    syslog(LOG_NOTICE,"cma event %d, error %d\n", event->event,
		    event->status);
	    cb->state = ERROR;
	    sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_UNREACHABLE:
	    syslog(LOG_NOTICE,"[%s]: RDMA_CM_EVENT_UNREACHABLE \n",__func__);
	    syslog(LOG_NOTICE,"cma event %d, error %d\n", event->event,
		    event->status);
	    cb->state = ERROR;
	    sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_REJECTED:
	    syslog(LOG_NOTICE,"[%s]: RDMA_CM_EVENT_REJECTED \n",__func__);
	    syslog(LOG_NOTICE,"cma event %d, error %d\n", event->event,
		    event->status);
	    cb->state = ERROR;
	    sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_DISCONNECTED:
	    syslog(LOG_NOTICE,"DISCONNECT EVENT...\n");
	    cb->state = ERROR;
	    sem_post(&cb->sem);
	    break;

	case RDMA_CM_EVENT_DEVICE_REMOVAL:
	    syslog(LOG_NOTICE,"cma detected device removal!!!!\n");
	    cb->state = ERROR;
	    sem_post(&cb->sem);
	    break;

	default:
	    syslog(LOG_NOTICE,"oof bad type!\n");
	    sem_post(&cb->sem);
	    break;
    }
    return 0;
}

static int read_done_postwork(struct krping_cb * cb, struct ibv_wc *wc)
{
    struct rdma_ctx *ctx;
    int i;
    unsigned long offset=0;

    ctx = (struct rdma_ctx *)ptr_from_uint64(wc->wr_id);	


    if(ctx->isDynamic){
	ibv_dereg_mr(ctx->dynamic_mr.rdma_mr);
	//TODO: finish below line
	ops.read_done(ctx->reqlist[0]->private_data);
    }else{
	for(i=0; i<ctx->numreq; i++){
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: ctxid:%d memcpy buf:%p, size:%ld, rdma_buf_offset:%ld \n",__func__,ctx->id,ctx->reqlist[i]->buf, ctx->reqlist[i]->size, offset);
	    memcpy(ctx->reqlist[i]->buf, ctx->mr->rdma_buf + offset, ctx->reqlist[i]->size);
	    offset = offset + ctx->reqlist[i]->size;
	    ops.read_done(ctx->reqlist[i]->private_data);
	}
	put_mr_freepool(ctx->cb->sess->mr_freepool,ctx->mr);
    }

    ctx->rb_index = -1;
    ctx->rb = NULL;
    ctx->reqlist[0] = NULL;
    ctx->numreq = 0;
    ctx->sending_item = NULL;
    atomic_set(ctx->ref_count,0);
    ctx->req_buf = NULL;
    ctx->len = 0;

    put_ctx_freepool(ctx);

    return 0;
}

// TODO : sending item
static int write_done_postwork(struct krping_cb * cb, struct ibv_wc *wc)
{
    struct rdma_ctx *ctx=NULL;
    struct local_page_list *sending_item=NULL;
    size_t i;
    int err;
    int cnt;
#ifdef DISKBACKUP
    int num_replica = NUM_REPLICA+1;
#else
    int num_replica = NUM_REPLICA;
#endif

    ctx = (struct rdma_ctx *)ptr_from_uint64(wc->wr_id);	
    if (ctx == NULL){
	syslog(LOG_NOTICE,"rdmabox[%s]: ctx is null or ctx->rb is null  \n",__func__);
	return 1;
    }

    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: \n", __func__);
    //TODO : bitmap set

    if(!ctx->sending_item){
	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: no sending item but req \n", __func__);
	for(i=0; i<ctx->numreq; i++){
	    ops.write_done(ctx->reqlist[i]->private_data);
	}

	if(ctx->isDynamic){
	    ibv_dereg_mr(ctx->dynamic_mr.rdma_mr);
	}else{
	    put_mr_freepool(ctx->cb->sess->mr_freepool,ctx->mr);
	}

    }else{ // from local mempool
	//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: sending item \n", __func__);
	sending_item = ctx->sending_item;
	if (!sending_item){
	    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: sending item is null  \n",__func__);
	    return 1;
	}
	cnt = atomic_inc_return(ctx->sending_item->ref_count);

	if(ctx->replica_index == 0 && cnt < num_replica ){
	    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: need to wait until replica is done cnt:%d num_replica:%d  \n",__func__,cnt,num_replica);
	    return 0;
	}

	if(cnt >= num_replica){
	    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: replica is done cnt:%d num_replica:%d  \n",__func__,cnt,num_replica);
	    for (i=0; i < sending_item->len;i++){
		struct tree_entry *entry;
		entry = sending_item->batch_list[i];
		atomic_dec(entry->ref_count);
		if(atomic_read(entry->ref_count)<=0){
		    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: entry ref_count is 0 \n",__func__);
		    clear_flag(entry,UPDATING);
		    atomic_set(entry->ref_count,0);
		}

		if(!test_flag(entry,RECLAIMABLE)){
		    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: put reclaimable list \n",__func__);
		    set_flag(entry,RECLAIMABLE);
		    err = put_reclaimable_index(entry);
		    //if(err)
		    //DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: fail to put reclaim list \n",__func__);
		}
	    }//for
	    put_free_item(sending_item);
	    put_mr_freepool(ctx->cb->sess->mr_freepool,ctx->mr);

	    if(ctx->replica_index != 0){
		ctx->prime_ctx->rb_index = -1;
		ctx->prime_ctx->rb = NULL;
		ctx->prime_ctx->reqlist[0] = NULL;
		ctx->prime_ctx->numreq = 0;
		ctx->prime_ctx->sending_item = NULL;
		atomic_set(ctx->prime_ctx->ref_count,0);
		ctx->prime_ctx->req_buf = NULL;
		ctx->prime_ctx->len = 0;

		//atomic_set(&ctx->prime_ctx->in_flight, CTX_IDLE);
		put_ctx_freepool(ctx->prime_ctx);
		//DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: secondary return primary index:%d, replica_index:%d \n",__func__,sending_item->start_index,ctx->replica_index);
	    }
	}
    }// from local mempool

out:
    // free its own ctx
    ctx->rb_index = -1;
    ctx->rb = NULL;
    ctx->reqlist[0] = NULL;
    ctx->numreq = 0;
    ctx->sending_item = NULL;
    atomic_set(ctx->ref_count,0);
    ctx->req_buf = NULL;
    ctx->len = 0;
    //atomic_set(&ctx->in_flight, CTX_IDLE);

    put_ctx_freepool(ctx);


    return 0;
}

static int recv_handler(struct krping_cb *cb, struct ibv_wc *wc)
{
    if (wc->byte_len != sizeof(cb->recv_buf)) {
	syslog(LOG_NOTICE,"Received bogus data, size %d\n",
		wc->byte_len);
	return -1;
    }

    switch (cb->recv_buf.type){
	case RESP_MR_MAP:
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: Received RESP_MR_MAP from node[%d] rb_index:%d \n", __func__,cb->cb_index,cb->recv_buf.rb_index);
	    cb->sess->cb_state_list[cb->cb_index] = CB_MAPPED;
	    remote_block_init(cb);
	    cb->state = RECV_MR_MAP;
	    sem_post(&cb->sem);
	    break;
	case RESP_QUERY:
	    DEBUG_LOG(LOG_NOTICE,"rdmabox[%s]: Received RESP_QUERY free_mem : %d, node:%d\n", __func__,cb->recv_buf.size_gb, cb->cb_index);
	    cb->rbh.target_size_gb = cb->recv_buf.size_gb;
	    cb->state = RECV_QUERY_REPL;	
	    sem_post(&cb->sem);
	    break;
	default:
	    syslog(LOG_NOTICE,"unknown received message : %d\n",cb->recv_buf.type);
	    return -1;
    }

    return 0;
}

#ifdef USE_HYBRID_POLL
void *cq_thread(void *arg)
{
    struct krping_cb *cb = arg;
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    int ret;
    struct ibv_wc *wc;
    struct ibv_wc wc_arr[MAX_POLL_WC];
    int num_wc=0;
    int max=0;
    int i;

    DEBUG_LOG(LOG_NOTICE,"cq_thread started.\n");

    while (1) {
	for(i=0; i<MAX_POLL_WC; i++){
	    ret = ibv_poll_cq(cb->cq, 1, &wc_arr[i]);
	    if (ret <= 0)
		break;
	    num_wc++;
	}

	for(i=0; i<num_wc; i++){
	    wc = &wc_arr[i];

	    ret = krping_cq_event_handler(cb, wc);
	    ibv_ack_cq_events(cb->cq, 1);
	    if (ret)
		pthread_exit(NULL);
	}//for

	if(num_wc == 0){
	    if(max >= RETRY){
		ret = ibv_req_notify_cq(cb->cq, 0);
		if (ret) {
		    syslog(LOG_NOTICE,stderr, "Failed to request notify cq event!\n");
		    pthread_exit(NULL);
		}

		ret = ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx);
		if (ret) {
		    syslog(LOG_NOTICE,stderr, "Failed to get cq event!\n");
		    pthread_exit(NULL);
		}
		continue;
	    }
	    max++;
	}
	num_wc=0;

    }//while
}
#endif

#ifdef USE_EVENT_POLL
void *cq_thread(void *arg)
{
    struct krping_cb *cb = arg;
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    int ret;
    struct ibv_wc wc;

    DEBUG_LOG(LOG_NOTICE,"cq_thread started.\n");

    while (1) {
	ret = ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx);
	if (ret) {
	    syslog(LOG_NOTICE,stderr, "Failed to get cq event!\n");
	    pthread_exit(NULL);
	}
	if (ev_cq != cb->cq) {
	    syslog(LOG_NOTICE,stderr, "Unknown CQ!\n");
	    pthread_exit(NULL);
	}
	ret = ibv_req_notify_cq(cb->cq, 0);
	if (ret) {
	    syslog(LOG_NOTICE,stderr, "Failed to set notify!\n");
	    pthread_exit(NULL);
	}

	while ((ret = ibv_poll_cq(cb->cq, 1, &wc)) == 1) {
	    ret = krping_cq_event_handler(cb, &wc);
	}
	ibv_ack_cq_events(cb->cq, 1);
	if (ret)
	    pthread_exit(NULL);
    }
}
#endif

#ifdef USE_BUSY_POLL
void *cq_thread(void *arg)
{
    struct krping_cb *cb = arg;
    struct ibv_wc wc;
    int ret;

    DEBUG_LOG(LOG_NOTICE,"cq_thread started.\n");

    while (1) {
	do {
	    ret = ibv_poll_cq(cb->cq, 1, &wc);
	} while (ret== 0);

	ret = krping_cq_event_handler(cb, &wc);
	ibv_ack_cq_events(cb->cq, 1);
	if (ret)
	    pthread_exit(NULL);
    }
}
#endif

int krping_cq_event_handler(struct krping_cb *cb, struct ibv_wc *wc)
{
    struct ibv_recv_wr *bad_wr;
    int ret=0;
    unsigned long idx;
    //long cnt;
    int i;
    struct rdma_ctx *ctx;

    if (wc->status) {
	if (wc->status == IBV_WC_WR_FLUSH_ERR) {
	    //syslog(LOG_NOTICE,"cq flushed\n");
	    return -1;
	} else {
	    syslog(LOG_NOTICE,"cq completion failed with wr_id %lx status %d opcode %d vender_err %x\n",
		    wc->wr_id, wc->status, wc->opcode, wc->vendor_err);
	    goto error;
	}
    }

    switch (wc->opcode) {
	case IBV_WC_SEND:
	    //DEBUG_LOG(LOG_NOTICE,"send completion\n");
	    break;

	case IBV_WC_RDMA_WRITE:
	    //DEBUG_LOG(LOG_NOTICE,"rdma write completion\n");
	    ret = write_done_postwork(cb, wc);
	    if (ret) {
		syslog(LOG_NOTICE,"rdmabox[%s]: write wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
		goto error;
	    }
	    break;

	case IBV_WC_RDMA_READ:
	    //DEBUG_LOG(LOG_NOTICE,"rdma read completion\n");
	    ret = read_done_postwork(cb, wc);
	    if (ret) {
		syslog(LOG_NOTICE,"rdmabox[%s]: read wc error: %d, cb->state=%d\n",__func__, ret, cb->state);
		goto error;
	    }
	    break;

	case IBV_WC_RECV:
	    //DEBUG_LOG(LOG_NOTICE,"recv completion\n");
	    ret = recv_handler(cb, wc);
	    if (ret) {
		syslog(LOG_NOTICE,"recv wc error: %d\n", ret);
		goto error;
	    }

	    ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	    if (ret) {
		syslog(LOG_NOTICE,"post recv error: %d\n", ret);
		goto error;
	    }

	    //sem_post(&cb->sem);
	    break;

	case IBV_WC_RECV_RDMA_WITH_IMM:
	    DEBUG_LOG(LOG_NOTICE,"IBV_WC_RECV_RDMA_WITH_IMM completion\n");
	    ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	    if (ret) {
		syslog(LOG_NOTICE,"post recv error: %d\n", 
			ret);
		goto error;
	    }
	    break;
	default:
	    syslog(LOG_NOTICE,
		    "%s:%d Unexpected opcode %d, Shutting down\n",
		    __func__, __LINE__, wc->opcode);
	    goto error;
    }

    if (ret) {
	syslog(LOG_NOTICE,"poll error %d\n", ret);
	goto error;
    }
    return 0;
error:
    cb->state = ERROR;
    //sem_post(&cb->sem);
    return -1;
}

int control_block_init(struct krping_cb *cb, struct session *session)
{
    int ret = 0;
    int i;

    cb->sess = session;
    cb->addr_type = AF_INET6;
    cb->state = IDLE;

    cb->rbh.block_size_gb = 0;
    cb->rbh.remote_block = (struct remote_block **)malloc(sizeof(struct remote_block *) * MAX_RB_NUM);
    cb->rbh.remote_mapped = (struct atomic_t *)malloc(sizeof(struct atomic_t) * MAX_RB_NUM);
    cb->rbh.rb_idx_map = (int *)calloc(MAX_RB_NUM, sizeof(int));
    cb->rbh.mig_rb_idx = (char *)calloc(MAX_RB_NUM,sizeof(char));
    for (i=0; i < MAX_RB_NUM; i++){
	atomic_init(cb->rbh.remote_mapped + i);
	atomic_set(cb->rbh.remote_mapped + i, RB_UNMAPPED);
	cb->rbh.rb_idx_map[i] = -1;
	cb->rbh.remote_block[i] = (struct remote_block *)malloc(sizeof(struct remote_block)); 
	cb->rbh.remote_block[i]->rb_state = ACTIVE; 
	cb->rbh.mig_rb_idx[i] = 0x00;
    }

    sem_init(&cb->sem, 0, 0);

    cb->cm_channel = rdma_create_event_channel();
    if (!cb->cm_channel) {
	syslog(LOG_NOTICE,"rdma_create_event_channel error\n");
	return -1;
    }

    ret = rdma_create_id(cb->cm_channel, &cb->cm_id, cb, RDMA_PS_TCP);
    if (ret) {
	syslog(LOG_NOTICE,"rdma_create_id error\n");
	return ret;
    }
    DEBUG_LOG(LOG_NOTICE,"created cm_id %p\n", cb->cm_id);

    pthread_create(&cb->cmthread, NULL, cm_thread, cb);
    return 0;
out:
    free(cb);
    return ret;
}

int get_cb_index(struct session *sess, struct portal *portal)
{
    int i=0,j=0;

    DEBUG_LOG(LOG_NOTICE,"[%s]: checking addr : %s %d\n",__func__, portal->addr, portal->port);

    for (i=0; i<sess->cb_num; i++) {
	if( strncmp(sess->portal_list[i].addr, portal->addr, sizeof(portal->addr)) == 0 && sess->portal_list[i].port == portal->port )
	{
	    DEBUG_LOG(LOG_NOTICE,"[%s]: Found match. portal[%d]->addr:%s portal[%d]->port:%d \n",__func__,i, sess->portal_list[i].addr, i, sess->portal_list[i].port );
	    j++;
	    break;
	}
    }

    if(j==0){
	syslog(LOG_NOTICE,"[%s]: match not found for %s %d\n",__func__, portal->addr, portal->port);
    }

    return i;
}

int setup_connection(struct session *sess, struct portal *portal, struct krping_cb *listener_cb)
{
    int i, ret =0;
    struct ibv_recv_wr *bad_wr;
    int cb_index = get_cb_index(sess, portal);
    struct krping_cb *tmp_cb;

    if (sess->cb_state_list[cb_index] > CB_IDLE) { // if state is larger than CB_IDLE, it has been connected before.
	DEBUG_LOG(LOG_NOTICE,"[%s]: connection already setup. incoming src addr : %s:%d cb_state:%d\n",__func__, portal->addr, portal->port, sess->cb_state_list[cb_index]);
    }else { // CB_IDLE
	tmp_cb = sess->cb_list[cb_index];
	tmp_cb->cm_id = listener_cb->child_cm_id;
	tmp_cb->cm_id->context = tmp_cb;

	tmp_cb->sess = sess;
	tmp_cb->addr_type = AF_INET6;
	tmp_cb->state = IDLE;

	ret = krping_setup_qp(tmp_cb, tmp_cb->cm_id);
	if (ret){
	    syslog(LOG_NOTICE,"[%s]: setup_qp failed: %d\n",__func__, ret);
	    return ret;
	}

	ret = krping_setup_buffers(tmp_cb);
	if (ret){
	    syslog(LOG_NOTICE,"[%s]: setup_buffers failed: %d\n",__func__, ret);
	    goto err1;
	}

	for(i=0; i<POST_RECV_DEPTH; i++){
	    ret = ibv_post_recv(tmp_cb->qp, &tmp_cb->rq_wr, &bad_wr);
	    if (ret) {
		syslog(LOG_NOTICE, "post recv error: %d\n", ret);
	    }
	}

	pthread_create(&tmp_cb->cqthread, NULL, cq_thread, tmp_cb);

	ret = krping_accept(tmp_cb,tmp_cb->cm_id);
	if (ret) {
	    syslog(LOG_NOTICE,"[%s]: accept failed: %d\n",__func__, ret);
	    goto err2;
	}

	sess->cb_state_list[cb_index] = CB_CONNECTED; //add CB_CONNECTED              
    }//CB_IDLE

    return 0;

err2:
    krping_free_buffers(tmp_cb);
err1:
    krping_free_qp(tmp_cb);  
}

int krping_accept(struct krping_cb *cb, struct rdma_cm_id *cm_id)
{
    struct rdma_conn_param conn_param;
    int ret;

    DEBUG_LOG(LOG_NOTICE,"accepting client connection request\n");

    memset(&conn_param, 0, sizeof conn_param);
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;

    ret = rdma_accept(cm_id, &conn_param);
    if (ret) {
	syslog(LOG_NOTICE, "rdma_accept error: %d\n", ret);
	return ret;
    }

    return 0;
}

static void krping_setup_wr(struct krping_cb *cb)
{
    cb->recv_sgl.addr = (uint64_t) (unsigned long) &cb->recv_buf;
    cb->recv_sgl.length = sizeof(struct message);
    cb->recv_sgl.lkey = cb->recv_mr->lkey;
    cb->rq_wr.sg_list = &cb->recv_sgl;
    cb->rq_wr.num_sge = 1;

    cb->send_sgl.addr = (uint64_t) (unsigned long) &cb->send_buf;
    cb->send_sgl.length = sizeof(struct message);
    cb->send_sgl.lkey = cb->send_mr->lkey;
    cb->sq_wr.opcode = IBV_WR_SEND;
    cb->sq_wr.send_flags = IBV_SEND_SIGNALED;
    cb->sq_wr.sg_list = &cb->send_sgl;
    cb->sq_wr.num_sge = 1;

    cb->rdma_sgl.addr = (uint64_t) (unsigned long) cb->rdma_buf;
    cb->rdma_sq_wr.send_flags = IBV_SEND_SIGNALED;
    cb->rdma_sq_wr.sg_list = &cb->rdma_sgl;
    cb->rdma_sq_wr.num_sge = 1;
}

int krping_setup_buffers(struct krping_cb *cb)
{
    int ret;

    DEBUG_LOG(LOG_NOTICE,"krping_setup_buffers called on cb %p message size:%ld\n", cb, sizeof(struct message));

    //cb->send_buf = malloc(sizeof(struct message));
    //cb->recv_buf = malloc(sizeof(struct message));
    cb->rdma_buf = malloc(RDMA_BUF_SIZE);

    cb->recv_mr = ibv_reg_mr(session.pd, &cb->recv_buf, sizeof(struct message),
	    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (!cb->recv_mr) {
	syslog(LOG_NOTICE,stderr, "recv_buf reg_mr failed\n");
	return errno;
    }

    cb->send_mr = ibv_reg_mr(session.pd, &cb->send_buf, sizeof(struct message),
	    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (!cb->send_mr) {
	syslog(LOG_NOTICE,stderr, "send_buf reg_mr failed\n");
	ret = errno;
	goto err1;
    }

    // user buf for test
    cb->rdma_mr = ibv_reg_mr(session.pd, cb->rdma_buf, RDMA_BUF_SIZE,
	    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (!cb->rdma_mr) {
	syslog(LOG_NOTICE,stderr, "rdma_buf reg_mr failed\n");
	ret = errno;
	goto err1;
    }

    krping_setup_wr(cb);
    DEBUG_LOG(LOG_NOTICE,"allocated & registered buffers...\n");
    return 0;
err1:
    if (cb->recv_mr)
	ibv_dereg_mr(cb->recv_mr);
    if (cb->send_mr)
	ibv_dereg_mr(cb->send_mr);
    if (cb->rdma_mr)
	ibv_dereg_mr(cb->rdma_mr);
    return ret;
}

void krping_free_buffers(struct krping_cb *cb)
{
    DEBUG_LOG(LOG_NOTICE,"krping_free_buffers called on cb %p\n", cb);

    if (cb->rdma_mr)
	ibv_dereg_mr(cb->rdma_mr);
}

int krping_create_qp(struct krping_cb *cb, struct rdma_cm_id *cm_id, struct session *session)
{
    struct ibv_qp_init_attr init_attr;
    int ret;

    struct ibv_device_attr dev_attr;
    ret = ibv_query_device(cm_id->verbs, &dev_attr);
    DEBUG_LOG(LOG_NOTICE,"max_sge:%d max_qp_wr:%d \n",dev_attr.max_sge,dev_attr.max_qp_wr);

    memset(&init_attr, 0, sizeof(init_attr));
    //init_attr.cap.max_send_wr = RPING_SQ_DEPTH;
    init_attr.cap.max_send_wr = dev_attr.max_qp_wr;
    //init_attr.cap.max_recv_wr = RPING_RQ_DEPTH;
    init_attr.cap.max_recv_wr = dev_attr.max_qp_wr;
    //init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_recv_sge = dev_attr.max_sge;
    //init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_send_sge = dev_attr.max_sge;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = cb->cq;
    init_attr.recv_cq = cb->cq;

    ret = rdma_create_qp(cm_id, session->pd, &init_attr);
    if (!ret)
	cb->qp = cm_id->qp;
    else
	syslog(LOG_NOTICE,"errno:%d\n",errno);

    return ret;
}

void krping_free_qp(struct krping_cb *cb)
{
    ibv_destroy_qp(cb->qp);
    ibv_destroy_cq(cb->cq);
    ibv_dealloc_pd(cb->sess->pd);
}

int krping_setup_qp(struct krping_cb *cb, struct rdma_cm_id *cm_id)
{
    int ret;

    if(!session.pd){
	session.pd = ibv_alloc_pd(cm_id->verbs);
	if (!session.pd) {
	    syslog(LOG_NOTICE,"[%s]: ibv_alloc_pd failed\n",__func__);
	    return -1;
	}
	DEBUG_LOG(LOG_NOTICE,"[%s]: created pd %p\n",__func__,session.pd);
    }

    cb->channel = ibv_create_comp_channel(cm_id->verbs);
    if (!cb->channel) {
	syslog(LOG_NOTICE,stderr, "ibv_create_comp_channel failed\n");
	ret = errno;
	goto err1;
    }
    DEBUG_LOG(LOG_NOTICE,"created channel %p\n", cb->channel);

    cb->cq = ibv_create_cq(cm_id->verbs,CQ_DEPTH, cb, cb->channel, 0);
    if (!cb->cq) {
	syslog(LOG_NOTICE,stderr, "ibv_create_cq failed\n");
	ret = errno;
	goto err2;
    }
    DEBUG_LOG(LOG_NOTICE,"created cq %p\n", cb->cq);

    ret = ibv_req_notify_cq(cb->cq, 0);
    if (ret) {
	syslog(LOG_NOTICE, "ibv_create_cq failed\n");
	goto err2;
    }

    ret = krping_create_qp(cb, cm_id, cb->sess);
    if (ret) {
	syslog(LOG_NOTICE, "krping_create_qp failed: %d\n", ret);
	goto err2;
    }
    DEBUG_LOG(LOG_NOTICE,"created qp %p\n", cb->qp);
    return 0;
err2:
    ibv_destroy_cq(cb->cq);
err1:
    ibv_dealloc_pd(cb->sess->pd);
    return ret;
}

static int krping_connect_client(struct krping_cb *cb)
{
    struct rdma_conn_param conn_param;
    int ret;
    //struct portal portal;
    int tmp_cb_index = -1;

    //memcpy(portal.addr, session.listener_portal.addr, sizeof(session.listener_portal.addr));
    //portal.port = session.listener_portal.port;
    //syslog(LOG_NOTICE,"rdmabox[%s]: conn_param portal: %s, %d\n",__func__, portal.addr, portal.port);

    memset(&conn_param, 0, sizeof conn_param);
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = 7;
    //conn_param.private_data = &portal;
    //conn_param.private_data_len = sizeof(struct portal);
    conn_param.private_data = (int*)&tmp_cb_index;;
    conn_param.private_data_len = sizeof(int);

    ret = rdma_connect(cb->cm_id, &conn_param);
    if (ret) {
	syslog(LOG_NOTICE, "rdma_connect error %d\n", ret);
	return ret;
    }

    sem_wait(&cb->sem);
    if (cb->state != CONNECTED) {
	syslog(LOG_NOTICE,stderr, "wait for CONNECTED state %d\n", cb->state);
	return -1;
    }
    DEBUG_LOG(LOG_NOTICE,"rdma_connect successful\n");
    return 0;
}

int krping_bind_client(struct krping_cb *cb)
{
    int ret;

    ret = rdma_resolve_addr(cb->cm_id, NULL, (struct sockaddr *) &cb->sin, 2000);
    if (ret) {
	syslog(LOG_NOTICE, "rdma_resolve_addr error %d\n", ret);
	return ret;
    }

    sem_wait(&cb->sem);
    if (cb->state != ROUTE_RESOLVED) {
	syslog(LOG_NOTICE,stderr, "waiting for addr/route resolution state %d\n",
		cb->state);
	return -1;
    }

    DEBUG_LOG(LOG_NOTICE,"rdma_resolve_addr - rdma_resolve_route successful\n");
    return 0;
}

void create_freepool(struct session *session)
{
    session->mr_freepool = (struct rdma_freepool_s *)malloc(sizeof(struct rdma_freepool_s));
    session->ctx_freepool = (struct rdma_freepool_s *)malloc(sizeof(struct rdma_freepool_s));

    if(rdma_freepool_create(session, &session->mr_freepool, NUM_MR, RDMA_BUF_SIZE))
	syslog(LOG_NOTICE,"fail to create rdma mr freepool\n");

    if(ctx_freepool_create(session, &session->ctx_freepool, NUM_CTX))
	syslog(LOG_NOTICE,"fail to create rdma ctx freepool\n");
}

void connect_remote(struct krping_cb *cb, struct session *session)
{
    int i;
    struct ibv_recv_wr *bad_wr;
    int ret;

    ret = krping_bind_client(cb);
    if (ret)
	return;

    ret = krping_setup_qp(cb, cb->cm_id);
    if (ret) {
	syslog(LOG_NOTICE, "setup_qp failed: %d\n", ret);
	return;
    }

    ret = krping_setup_buffers(cb);
    if (ret) {
	syslog(LOG_NOTICE, "krping_setup_buffers failed: %d\n", ret);
	goto err1;
    }

    for(i=0; i<POST_RECV_DEPTH; i++){
	ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
	if (ret) {
	    syslog(LOG_NOTICE, "post recv error: %d\n", ret);
	}
    }

    pthread_create(&cb->cqthread, NULL, cq_thread, cb);

    ret = krping_connect_client(cb);
    if (ret) {
	syslog(LOG_NOTICE, "connect error %d\n", ret);
	goto err2;
    }

    return;

err2:
    krping_free_buffers(cb);
err1:
    krping_free_qp(cb);
}

static int bind_server(struct krping_cb *listener_cb)
{
    int ret;
    pthread_t client_thread;

    listener_cb->sin.sin_port = listener_cb->port;
    ret = rdma_bind_addr(listener_cb->cm_id, (struct sockaddr *)&listener_cb->sin);
    if (ret) {
	syslog(LOG_NOTICE,"[%s]: rdma_bind_addr error %d\n",__func__, ret);
	return ret;
    }
    DEBUG_LOG(LOG_NOTICE,"[%s]: rdma_bind_addr successful\n",__func__);
    DEBUG_LOG(LOG_NOTICE,"[%s]: rdma_listen\n",__func__);

    ret = rdma_listen(listener_cb->cm_id, 3);
    if (ret) {
	syslog(LOG_NOTICE,"[%s]: rdma_listen failed: %d\n",__func__, ret);
	return ret;
    }

    return 0;
}

static int listener_cb_fn(struct session *sess)
{
    struct krping_cb *listener_cb = sess->listener_cb;
    struct rdma_cm_event *event;
    int ret = 0;

    listener_cb->sess = sess;
    listener_cb->addr_type = AF_INET6;
    listener_cb->state = IDLE;
    sem_init(&listener_cb->sem, 0, 0);

    DEBUG_LOG(LOG_NOTICE,"[%s] start listener\n",__func__);

    listener_cb->cm_channel = rdma_create_event_channel();
    if (!listener_cb->cm_channel) {
	ret = errno;
	syslog(LOG_NOTICE,stderr, "rdma_create_event_channel error %d\n", ret);
	return ret;
    }

    ret = rdma_create_id(listener_cb->cm_channel, &listener_cb->cm_id, listener_cb, RDMA_PS_TCP);
    if (ret) {
	ret = errno;
	syslog(LOG_NOTICE,stderr, "rdma_create_id error %d\n", ret);
	return ret;
    }
    DEBUG_LOG(LOG_NOTICE,"[%s]: created cm_id %p for listener_cb \n", __func__, listener_cb->cm_id);

    bind_server(listener_cb);

    while (1) {
	ret = rdma_get_cm_event(listener_cb->cm_channel, &event);
	if (ret) {
	    syslog(LOG_NOTICE,stderr, "rdma_get_cm_event err %d\n", ret);
	    exit(ret);
	}
	struct rdma_cm_event event_copy;

	memcpy(&event_copy, event, sizeof(*event));
	rdma_ack_cm_event(event);

	ret = krping_cma_event_handler(&event_copy);
	if (ret)
	    exit(ret);
	listener_cb->state = IDLE;
    }

    DEBUG_LOG(LOG_NOTICE,"destroy cm_id %p\n", listener_cb->cm_id);
    rdma_destroy_id(listener_cb->cm_id);
    rdma_destroy_event_channel(listener_cb->cm_channel);
    free(listener_cb);
}

int get_addr(char *dst, struct sockaddr_in *addr)
{
    struct addrinfo *res;
    int ret;

    ret = getaddrinfo(dst, NULL, NULL, &res);
    if (ret) {
	syslog(LOG_NOTICE,"getaddrinfo failed - invalid hostname or IP address\n");
	return ret;
    }

    if (res->ai_family != PF_INET) {
	ret = -1;
	goto out;
    }

    *addr = *(struct sockaddr_in *) res->ai_addr;
out:
    freeaddrinfo(res);
    return ret;
}

void portal_parser(FILE *fptr)
{
    char line[256];
    int p_count=0, i=0, j=0;
    int port;

    fgets(line, sizeof(line), fptr);
    sscanf(line, "%d", &p_count);
    session.cb_num = p_count;
    session.portal_list = malloc(sizeof(struct portal) * p_count);
    syslog(LOG_NOTICE,"[%s]: num_servers : %d \n",__func__,session.cb_num);

    while (fgets(line, sizeof(line), fptr)) {
	syslog(LOG_NOTICE,"%s", line);
	j = 0;
	while (*(line + j) != ':'){
	    j++;
	}
	memcpy(session.portal_list[i].addr, line, j);
	session.portal_list[i].addr[j] = '\0';
	port = 0;
	sscanf(line+j+1, "%d", &port);
	session.portal_list[i].port = (uint16_t)port;
	syslog(LOG_NOTICE,"daemon[%s]: portal: %s, %d\n",__func__, session.portal_list[i].addr, session.portal_list[i].port);
	i++;
    }
}

int session_create(struct session *session, int num_block)
{
    int i, j, k, ret;
    struct krping_cb *cb;
    struct krping_cb *replica_cb_list[NUM_REPLICA];


    LOCAL_BLOCKS_PER_REMOTE_BLOCK = REMOTE_BLOCK_SIZE / LOCAL_BLOCK_SIZE;

    session->mr_freepool = NULL;
    //session->rdma_write_freepool = (struct rdma_freepool_s *)malloc(sizeof(struct rdma_freepool_s));
    //session->rdma_read_freepool = (struct rdma_freepool_s *)malloc(sizeof(struct rdma_freepool_s));
    session->cb_list = (struct krping_cb **)malloc(sizeof(struct krping_cb *) * session->cb_num);
    session->cb_state_list = (enum cb_state *)malloc(sizeof(enum cb_state) * session->cb_num);
    pthread_spin_init(&session->cb_lock,0);

    for (i=0; i<session->cb_num; i++) {
	DEBUG_LOG(LOG_NOTICE,"cbinit - cb_index:%d addr:%s port:%d\n",i,session->portal_list[i].addr,session->portal_list[i].port);
	session->cb_state_list[i] = CB_IDLE;
	session->cb_list[i] = malloc(sizeof(struct krping_cb));
	session->cb_list[i]->port = htons(session->portal_list[i].port);
	inet_pton(AF_INET6, session->portal_list[i].addr, session->cb_list[i]->addr);
	session->cb_list[i]->cb_index = i;
	get_addr(session->portal_list[i].addr, &session->cb_list[i]->sin);
	session->cb_list[i]->sin.sin_port = htons(session->portal_list[i].port);
	control_block_init(session->cb_list[i], session);
    }

    k = num_block / LOCAL_BLOCKS_PER_REMOTE_BLOCK;
    if( num_block % LOCAL_BLOCKS_PER_REMOTE_BLOCK > 0){
	k++;
    }
    syslog(LOG_NOTICE,"session_create - num_block:%d num_remote_block:%d\n",num_block, k);

    for (i = 0; i < NUM_REPLICA; i++) {
	session->cb_index_map[i] = malloc(sizeof(struct atomic_t) * k);
	if(!session->cb_index_map[i]){
	    syslog(LOG_NOTICE,"session_create - fail to malloc cb_index_map \n");
	}
	session->mapping_lbg_to_rb[i] = (int*)malloc(sizeof(int) * k); // local block to remote block
	if(!session->mapping_lbg_to_rb[i]){
	    syslog(LOG_NOTICE,"session_create - fail to malloc mapping_lbg_to_rb \n");
	}

	for (j = 0; j < k; j++){
	    atomic_init(session->cb_index_map[i] + j);
	    atomic_set(session->cb_index_map[i] + j, NO_CB_MAPPED);
	    session->mapping_lbg_to_rb[i][j] = -1;
	}
    }

    session->wr_req_q = malloc(sizeof(lfqueue_t));
    if (lfqueue_init(session->wr_req_q) == -1){
	syslog(LOG_NOTICE,"session_create - fail to malloc wr_req_q \n");
	return -1;
    }
    session->rd_req_q = malloc(sizeof(lfqueue_t));
    if (lfqueue_init(session->rd_req_q) == -1){
	syslog(LOG_NOTICE,"session_create - fail to malloc rd_req_q \n");
	return -1;
    }

    if(!session->mr_freepool)
	create_freepool(session);

    mempool_init();

    brd_init();

    /*
    // create mr pool
    if(rdma_freepool_create(session, &session->rdma_write_freepool, NUM_MR_FREEPOOL_WR, WR_MR_SIZE)){
    syslog(LOG_NOTICE,"fail to create rdma write freepool\n");
    }
    if(rdma_freepool_create(session, &session->rdma_read_freepool, NUM_MR_FREEPOOL_RD, RD_MR_SIZE)){
    syslog(LOG_NOTICE,"fail to create rdma read freepool\n");
    }
     */


#ifdef USE_RDMABOX_WRITEBACK
    //sem_init(&session->sending_list_sem, 0, 0);
    pthread_create(&session->remote_sender_th, NULL, remote_sender_full_fn, session);
#endif

    // pre-conn
    for(i=0; i<10; i++)
    {
        get_cb(session, i, replica_cb_list);
    }

    return 0;
}

int listener_start(struct session *session)
{
    session->pd = NULL;

    inet_pton(AF_INET6, session->listener_portal.addr, session->listener_cb->addr);
    session->listener_cb->port = htons(session->listener_portal.port);

    DEBUG_LOG(LOG_NOTICE,"[%s]: create listener thread \n",__func__);
    listener_cb_fn(session);
}

void read_portal_file(char *filepath)
{
    FILE *fptr;

    DEBUG_LOG(LOG_NOTICE,"reading portal file.. [%s]\n",filepath);
    if ((fptr = fopen(filepath,"r")) == NULL){
	syslog(LOG_NOTICE,"portal file open failed.");
	exit(1);
    }

    portal_parser(fptr);

    fclose(fptr);
}

void* client_thread_fn(void* data)
{
    int i;
    int input;
    struct krping_cb *cb = data;
    struct krping_cb *tmp_cb;
    struct krping_cb *replica_cb_list[NUM_REPLICA];

    usleep(1000000);

    while(1){

	syslog(LOG_NOTICE,"Enter option\n");
	syslog(LOG_NOTICE,"1: connect client\n");
	syslog(LOG_NOTICE,"2: start test\n");
	scanf("%d", &input);
	while ((getchar()) != '\n');
	syslog(LOG_NOTICE,"entered number : %d \n", input);

	switch(input){
	    case 1:
		tmp_cb = session.cb_list[0];
		syslog(LOG_NOTICE,"[%s] connect node:0\n",__func__);
		connect_remote(tmp_cb, &session);
		/*
		   for (i=0; i<session.cb_num; i++) {
		   syslog(LOG_NOTICE,"[%s] connect node:%d\n",__func__,i);
		   tmp_cb = session.cb_list[i];
		   connect_remote(tmp_cb, &session);
		   }
		 */
		break;
	    case 2:
		tmp_cb = get_cb(&session, 0, replica_cb_list);
		if(!tmp_cb){
		    syslog(LOG_NOTICE,"brd[%s]: tmp_cb is null \n", __func__);
		}
		break;
	    default:
		syslog(LOG_NOTICE,"wrong number\n");
		break;
	}

    }

}	

int init(char *addr, int port, char *portal, int num_block)
{
    int i;
    int ret;
    pthread_t client_thread;
    struct krping_cb *cb;
    cb = (struct krping_cb *)malloc(sizeof(struct krping_cb));   
    if (!cb)
	return -ENOMEM;

    setlogmask (LOG_UPTO (LOG_NOTICE));
    openlog ("rdmabox", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);

    DEBUG_LOG(LOG_NOTICE,"listener_cb created %p\n", cb);

    DEBUG_LOG(LOG_NOTICE,"test addr:%s, port:%d, portal:%s\n",addr, port, portal);


    memcpy(session.listener_portal.addr, addr, sizeof(session.listener_portal.addr));
    session.listener_portal.addr[sizeof(session.listener_portal.addr)-1] = '\0';
    ret = get_addr(addr, &cb->sin);

    cb->port = port;
    session.listener_portal.port = port;
    DEBUG_LOG(LOG_NOTICE,"port %d\n", port);

    read_portal_file(portal);

    session.listener_cb = cb;

    session_create(&session, num_block);
    /*
    //TODO : this pre-conn causes block in write without crash. This should be fixed later.
    struct krping_cb *replica_cb_list[NUM_REPLICA];
    //pre-conn
    for(i=0; i<2; i++){
    get_cb(&session, i, replica_cb_list);
    usleep(100);
    }
     */
    syslog(LOG_NOTICE,"rdmabox init done\n");
    /*
       listener_start(&session); //block here
       syslog(LOG_NOTICE,"destroy cm_id %p\n", cb->cm_id);
       rdma_destroy_id(cb->cm_id);
out:
rdma_destroy_event_channel(cb->cm_channel);
free(cb);
     */
    return 0;
}
/*
   int main(int argc, char *argv[])
   {
   int ret;
   pthread_t client_thread;
   struct krping_cb *cb;
//create listener_cb
cb = (struct krping_cb *)malloc(sizeof(struct krping_cb));   
if (!cb)
return -ENOMEM;
syslog(LOG_NOTICE,"//listener_cb created %p\n", cb);

int op;
int opterr = 0;
while ((op=getopt(argc, argv, "a:p:i:")) != -1) {
switch (op) {
case 'a':
memcpy(session.listener_portal.addr, optarg, sizeof(session.listener_portal.addr));
session.listener_portal.addr[sizeof(session.listener_portal.addr)-1] = '\0';
ret = get_addr(optarg, &cb->sin);
break;
case 'p':
cb->port = htons(atoi(optarg));
session.listener_portal.port = atoi(optarg);
syslog(LOG_NOTICE,"port %d\n", (int) atoi(optarg));
break;
case 'i':
read_portal_file(optarg);
break;
default:
ret = EINVAL;
goto out;
}
}
if (ret)
goto out;

session.listener_cb = cb;

session_create(&session, NUM_BLOCK);

pthread_create(&client_thread, NULL, client_thread_fn, cb);

listener_start(&session); //block here

syslog(LOG_NOTICE,"destroy cm_id %p\n", cb->cm_id);
rdma_destroy_id(cb->cm_id);
out:
rdma_destroy_event_channel(cb->cm_channel);
free(cb);
return 0;
}
 */
