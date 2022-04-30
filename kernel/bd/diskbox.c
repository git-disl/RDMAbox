/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
 *
 *
 * Stackbd
 * Copyright 2014 Oren Kishon
 * https://github.com/OrenKishon/stackbd
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
 *        copyright notice, this list of conditions and the following
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

#include "rpg_drv.h"
#include "rdmabox.h"
#include "diskbox.h"
#include "radixtree.h"
#include "rpg_mempool.h"
#include <linux/smp.h>

#define DEBUG

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
extern struct bio_set *io_bio_set;
#endif

#ifdef LAT_DEBUG
struct lat_debug {
    struct timeval starttime;
    struct request *req;
};
#endif

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
#define HAVE_LOOKUP_BDEV_PATCH
#endif

/* lookup_bdev patch: https://www.redhat.com/archives/dm-devel/2016-April/msg00372.html */
#ifdef HAVE_LOOKUP_BDEV_PATCH
#define LOOKUP_BDEV(x) lookup_bdev(x, 0)
#else
#define LOOKUP_BDEV(x) lookup_bdev(x)
#endif

struct bio * create_bio_copy(struct bio *src)
{
   int  status;
   struct bio *dst;

   //Allocate a memory for a new 'bio' structure 
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 15, 0)
   if ( !(dst = bio_clone_fast(src, GFP_KERNEL, io_bio_set)) ){
#else
   if ( !(dst = bio_clone(src, GFP_KERNEL)) ){
#endif
        printk("bio_clone_bioset() -> NULL\n");
        return NULL;
   }
   if ( status = bio_alloc_pages(dst , GFP_KERNEL)){
        printk("bio_alloc_pages() -> %d\n", status);
        return NULL;
   }

   return dst;
}

void RDMABOX_stackbd_end_io(struct bio *bio, int err)
{
#ifdef LAT_DEBUG
  struct timeval end;
  unsigned long diff;
  struct lat_debug *ctx = (struct lat_debug *)ptr_from_uint64(bio->bi_private);
  struct request *req = ctx->req;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
  blk_mq_end_request(req, err);
#else
  blk_mq_end_io(req, err);
#endif

  int write = rq_data_dir(req);
  do_gettimeofday(&end);
  diff=(end.tv_sec - ctx->starttime.tv_sec)*1000000 + (end.tv_usec - ctx->starttime.tv_usec);
  if(write){
      printk("WR disk %ld usec\n",diff);
  }else{
      printk("RD disk %ld usec\n",diff);
  }
  kfree(ctx);
#else// LAT_DEBUG
  struct request *req = (struct request *)ptr_from_uint64(bio->bi_private);

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
  blk_mq_end_request(req, err);
#else
  blk_mq_end_io(req, err);
#endif

#endif
}

void RDMABOX_stackbd_end_io3(struct bio *bio, int err)
{
  struct rdma_ctx *ctx=NULL;
  struct local_page_list *sending_item=NULL;
  size_t i;
  int ret;
  int cnt;
  int cbindex;
  int cpuindex;
#ifdef DISKBACKUP
  int num_replica = NUM_REPLICA+1;
#else
  int num_replica = NUM_REPLICA;
#endif

  ctx = (struct rdma_ctx *)ptr_from_uint64(bio->bi_private);
  if (ctx == NULL){
    printk("diskbox[%s]: ctx is null \n",__func__);
    return;
  }
#ifdef LAT_DEBUG
  struct timeval end;
  do_gettimeofday(&end);
  diff=(end.tv_sec - ctx->starttime.tv_sec)*1000000 + (end.tv_usec - ctx->starttime.tv_usec);
  printk("WR diskbackup %ld usec\n",diff);
#endif

  if(!ctx->sending_item){
      cnt = atomic_inc_return(&ctx->prime_ctx->ref_count);

      // prime ctx
      if(ctx->replica_index == 0 && cnt < num_replica ){
          //printk("diskbox[%s]: return disk replica_index:%d \n",__func__,ctx->replica_index);
          return;
      }

      // last ctx 
      if(cnt >= num_replica){
#if defined(USE_SGE_WRITE) || defined(SUPPORT_SGE_WRITE_BATCH)
          //printk("diskbox[%s]: unmap sg and end io replica_index:%d \n",__func__,ctx->replica_index);
          dma_unmap_sg(ctx->cb->RDMABOX_sess->pd->device->dma_device, ctx->data_tbl.sgl, ctx->data_tbl.nents, DMA_TO_DEVICE);
          for(i=0; i<ctx->numreq; i++){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
              blk_mq_end_request(ctx->reqlist[i], 0);
#else
              blk_mq_end_io(ctx->reqlist[i], 0);
#endif
          }//for
#endif//SGE
          if(!ctx->prime_ctx->isSGE){
/*
              cpuindex = ctx->prime_ctx->mr->cpuhint;
              cbindex = ctx->prime_ctx->mr->cbhint;
              ctx->prime_ctx->mr->cpuhint = -1;
              ctx->prime_ctx->mr->cbhint = -1;
              put_freepool(ctx->cb->RDMABOX_sess->cb_list[cbindex]->rdma_write_freepool_list[cpuindex],NULL,ctx->prime_ctx->mr);
*/
              put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->prime_ctx->mr->cbhint]->rdma_write_freepool_list[ctx->prime_ctx->mr->cpuhint],NULL,ctx->prime_ctx->mr);
          }

          if(ctx->replica_index != 0){
              ctx->prime_ctx->sending_item = NULL;
              ctx->prime_ctx->chunk_index = -1;
              ctx->prime_ctx->chunk_ptr = NULL;
              atomic_set(&ctx->prime_ctx->in_flight, CTX_IDLE);
/*
              cpuindex = ctx->prime_ctx->cpuhint;
              cbindex = ctx->prime_ctx->cbhint;
              ctx->prime_ctx->cpuhint = -1;
              ctx->prime_ctx->cbhint = -1;
	      put_freepool(ctx->cb->RDMABOX_sess->cb_list[cbindex]->rdma_write_freepool_list[cpuindex],ctx->prime_ctx,NULL);
*/
	      put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->prime_ctx->cbhint]->rdma_write_freepool_list[ctx->prime_ctx->cpuhint],ctx->prime_ctx,NULL);
              //printk("diskbox[%s]: secondary return primary replica_index:%d \n",__func__,ctx->replica_index);
          }
     }// last ctx
  }else{ // from local mempool
      sending_item = ctx->sending_item;
      if (unlikely(!sending_item)){
          printk("diskbox[%s]: sending item is null  \n",__func__);
          return;
      }
      cnt = atomic_inc_return(&ctx->sending_item->ref_count);

      if(ctx->replica_index == 0 && cnt < num_replica )
          return;

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
                  printk("diskbox[%s]: fail to put reclaim list \n",__func__);
          }
        }//for

        put_free_item(sending_item);
/*
        cpuindex = ctx->prime_ctx->mr->cpuhint;
        cbindex = ctx->prime_ctx->mr->cbhint;
        ctx->prime_ctx->mr->cpuhint = -1;
        ctx->prime_ctx->mr->cbhint = -1;
        put_freepool(ctx->cb->RDMABOX_sess->cb_list[cbindex]->rdma_write_freepool_list[cpuindex],NULL,ctx->prime_ctx->mr);
*/
        put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->prime_ctx->mr->cbhint]->rdma_write_freepool_list[ctx->prime_ctx->mr->cpuhint],NULL,ctx->prime_ctx->mr);

        if(ctx->replica_index != 0){
           ctx->prime_ctx->sending_item = NULL;
           ctx->prime_ctx->chunk_index = -1;
           ctx->prime_ctx->chunk_ptr = NULL;
           atomic_set(&ctx->prime_ctx->in_flight, CTX_IDLE);
/*
           cpuindex = ctx->prime_ctx->cpuhint;
           cbindex = ctx->prime_ctx->cbhint;
           ctx->prime_ctx->cpuhint = -1;
           ctx->prime_ctx->cbhint = -1;
           put_freepool(ctx->cb->RDMABOX_sess->cb_list[cbindex]->rdma_write_freepool_list[cpuindex],ctx->prime_ctx,NULL);
*/
           put_freepool(ctx->cb->RDMABOX_sess->cb_list[ctx->prime_ctx->cbhint]->rdma_write_freepool_list[ctx->prime_ctx->cpuhint],ctx->prime_ctx,NULL);
           //printk("diskbox[%s]: secondary return primary index:%d, replica_index:%d \n",__func__,sending_item->start_index,ctx->replica_index);
        }
     }
#ifdef LAT_DEBUG
     do_gettimeofday(&end);
     diff=(end.tv_sec - ctx->starttime.tv_sec)*1000000 + (end.tv_usec - ctx->starttime.tv_usec);
     printk("WR remote RDMA %ld usec\n",diff);
#endif
  }// from local mempool
out:
  // free its own ctx
  ctx->sending_item = NULL;
  ctx->chunk_index = -1;
  ctx->chunk_ptr = NULL;
  ctx->numreq = 0;
  atomic_set(&ctx->in_flight, CTX_IDLE);
  cpuindex = ctx->prime_ctx->cpuhint;
  cbindex = ctx->prime_ctx->cbhint;
  ctx->prime_ctx->cpuhint = -1;
  ctx->prime_ctx->cbhint = -1;
  put_freepool(ctx->cb->RDMABOX_sess->cb_list[cbindex]->rdma_write_freepool_list[cpuindex],ctx->prime_ctx,NULL);
  //printk("diskbox[%s]: remote write done index:%d, replica_index:%d \n",__func__,sending_item->start_index,ctx->replica_index);

  return;
}

static void stackbd_io_fn(struct bio *bio)
{
  /*
  if (bio == NULL){
    printk("rpg[%s]: bio is NULL\n",__func__);
  }
  */
#if LINUX_VERSION_CODE <= KERNEL_VERSION(3, 14, 0)
  bio->bi_bdev = stackbd.bdev_raw;
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 15, 0)
  trace_block_bio_remap(bdev_get_queue(stackbd.bdev_raw), bio, bio_dev(bio), 
#else
  trace_block_bio_remap(bdev_get_queue(stackbd.bdev_raw), bio, bio->bi_bdev->bd_dev, 
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 14, 0)
      bio->bi_iter.bi_sector);
#else
      bio->bi_sector);
#endif 

  generic_make_request(bio);
}
static int stackbd_threadfn(void *data)
{
  struct bio *bio;

  set_user_nice(current, -20);
  while (!kthread_should_stop())
  {
    wait_event_interruptible(req_event, kthread_should_stop() ||
	!bio_list_empty(&stackbd.bio_list));
    spin_lock_irq(&stackbd.lock);
    if (bio_list_empty(&stackbd.bio_list))
    {
      spin_unlock_irq(&stackbd.lock);
      continue;
    }
    bio = bio_list_pop(&stackbd.bio_list);
    spin_unlock_irq(&stackbd.lock);
    stackbd_io_fn(bio);
  }
  return 0;
}

void stackbd_make_request5(struct bio *bio)
{
  spin_lock_irq(&stackbd.lock);
  if (!stackbd.bdev_raw)
  {
    printk("rpg[%s]: Request before bdev_raw is ready, aborting\n",__func__);
    goto abort;
  }
  if (!stackbd.is_active)
  {
    printk("rpg[%s]: Device not active yet, aborting\n",__func__);
    goto abort;
  }
  bio->bi_end_io = (bio_end_io_t*)RDMABOX_stackbd_end_io3;
  bio_list_add(&stackbd.bio_list, bio);

  wake_up(&req_event);
  spin_unlock_irq(&stackbd.lock);
  return;
abort:
  spin_unlock_irq(&stackbd.lock);
  printk("rpg[%s]: <%p> Abort request\n",__func__, bio);
  bio_io_error(bio);
}

void stackbd_make_request2(struct request_queue *q, struct request *req)
{
  struct bio *bio = NULL;
  struct bio *b = req->bio;
  int i;
  int len = req->nr_phys_segments;

#ifdef LAT_DEBUG
  struct lat_debug * ctx;
  ctx = (struct lat_debug *)kzalloc(sizeof(struct lat_debug), GFP_KERNEL);
  do_gettimeofday(&ctx->starttime);
  ctx->req = req;
#endif

#ifdef DEBUG
  //printk("rpg %s: request nr_phys_segments=%d\n",__func__,len);
#endif
  if (!stackbd.bdev_raw)
  {
    printk("rpg[%s]: Request before bdev_raw is ready, aborting\n",__func__);
    goto abort;
  }
  if (!stackbd.is_active)
  {
    printk("rpg[%s]: Device not active yet, aborting\n",__func__);
    goto abort;
  }
  for (i=0; i<len -1; i++){
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 15, 0)
    bio = bio_clone_fast(b, GFP_ATOMIC, io_bio_set);
#else
    bio = bio_clone(b, GFP_ATOMIC);
#endif
    spin_lock_irq(&stackbd.lock);
    bio_list_add(&stackbd.bio_list, bio);
    spin_unlock_irq(&stackbd.lock);
    b = b->bi_next;
  }
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 15, 0)
  bio = bio_clone_fast(b, GFP_ATOMIC, io_bio_set);
#else
  bio = bio_clone(b, GFP_ATOMIC);
#endif
  bio->bi_end_io = (bio_end_io_t*)RDMABOX_stackbd_end_io;
#ifdef LAT_DEBUG
  bio->bi_private = uint64_from_ptr(ctx);
#else
  bio->bi_private = uint64_from_ptr(req);
#endif
  spin_lock_irq(&stackbd.lock);
  bio_list_add(&stackbd.bio_list, bio);
  wake_up(&req_event);
  spin_unlock_irq(&stackbd.lock);
  return;
abort:
  bio_io_error(b);
  printk("rpg[%s]: <%p> Abort request\n\n",__func__, bio);
}

// from original stackbd
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
blk_qc_t stackbd_make_request(struct request_queue *q, struct bio *bio)
#else
void stackbd_make_request(struct request_queue *q, struct bio *bio)
#endif
{
  if (!stackbd.bdev_raw)
  {
    printk("rpg[%s]: Request before bdev_raw is ready, aborting\n",__func__);
    goto abort;
  }
  if (!stackbd.is_active)
  {
    printk("rpg[%s]: Device not active yet, aborting\n",__func__);
    goto abort;
  }
  spin_lock_irq(&stackbd.lock);
  bio_list_add(&stackbd.bio_list, bio);
  wake_up(&req_event);
  spin_unlock_irq(&stackbd.lock);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
  return 0;
#else
  return;
#endif
abort:
  printk("rpg[%s]: <%p> Abort request\n\n",__func__, bio);
  bio_io_error(bio);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
  return 0;
#endif
}

static struct block_device *stackbd_bdev_open(char dev_path[])
{
  /* Open underlying device */
  struct block_device *bdev_raw = LOOKUP_BDEV(dev_path);
#ifdef DEBUG
  printk("rpg[%s]: Opened %s\n",__func__, dev_path);
#endif
  if (IS_ERR(bdev_raw))
  {
    printk("rpg[%s]: error opening raw device <%lu>\n",__func__, PTR_ERR(bdev_raw));
    return NULL;
  }
  if (!bdget(bdev_raw->bd_dev))
  {
    printk("rpg[%s]: error bdget()\n",__func__);
    return NULL;
  }
  if (blkdev_get(bdev_raw, STACKBD_BDEV_MODE, &stackbd))
  {
    printk("rpg[%s]: error blkdev_get()\n",__func__);
    bdput(bdev_raw);
    return NULL;
  }
  return bdev_raw;
}

int stackbd_start(char dev_path[])
{
  unsigned max_sectors;
  unsigned int page_sec = RDMABOX_PAGE_SIZE;

  if (!(stackbd.bdev_raw = stackbd_bdev_open(dev_path)))
    return -EFAULT;
  /* Set up our internal device */
  stackbd.capacity = get_capacity(stackbd.bdev_raw->bd_disk);
  printk("rpg[%s]: Device real capacity: %llu\n",__func__, (long long unsigned int) stackbd.capacity);
  set_capacity(stackbd.gd, stackbd.capacity-46139392);
  sector_div(page_sec, KERNEL_SECTOR_SIZE);
  max_sectors = page_sec * RDMA_WR_BUF_LEN;
  blk_queue_max_hw_sectors(stackbd.queue, max_sectors);
  printk("rpg[%s]: Max sectors: %u \n",__func__, max_sectors);
  stackbd.thread = kthread_create(stackbd_threadfn, NULL,stackbd.gd->disk_name);
  if (IS_ERR(stackbd.thread))
  {
    printk("rpg[%s]: error kthread_create <%lu>\n",__func__,PTR_ERR(stackbd.thread));
    goto error_after_bdev;
  }
  printk("rpg[%s]: done initializing successfully\n",__func__);
  stackbd.is_active = 1;
  atomic_set(&stackbd.redirect_done, STACKBD_REDIRECT_OFF);
  wake_up_process(stackbd.thread);
  return 0;
error_after_bdev:
  blkdev_put(stackbd.bdev_raw, STACKBD_BDEV_MODE);
  bdput(stackbd.bdev_raw);
  return -EFAULT;
}

int stackbd_getgeo(struct block_device * block_device, struct hd_geometry * geo)
{
  long size;
  size = stackbd.capacity * (LOGICAL_BLOCK_SIZE / KERNEL_SECTOR_SIZE);
  geo->cylinders = (size & ~0x3f) >> 6;
  geo->heads = 4;
  geo->sectors = 16;
  geo->start = 0;
  return 0;
}

void RDMABOX_mq_request_stackbd(struct request *req)
{
  stackbd_make_request2(stackbd.queue, req);
}

static struct block_device_operations stackbd_ops = {
  .owner           = THIS_MODULE,
  .getgeo      = stackbd_getgeo,
};

int register_stackbd_device(int major_num)
{
  int err = 0;

#ifdef DEBUG
  printk("rpg[%s]: start stackbd register \n", __func__);
#endif

  spin_lock_init(&stackbd.lock);
  if (!(stackbd.queue = blk_alloc_queue(GFP_KERNEL)))
  {
    printk("rpg %s:stackbd: alloc_queue failed\n",__func__);
    return -EFAULT;
  }
  blk_queue_make_request(stackbd.queue, stackbd_make_request);
  blk_queue_logical_block_size(stackbd.queue, LOGICAL_BLOCK_SIZE);
  if ((major_num = register_blkdev(major_num, STACKBD_NAME)) < 0)
  {
    printk("rpg %s:stackbd: unable to get major number\n",__func__);
    err=-EFAULT;
    goto error_after_alloc_queue;
  }
  if (!(stackbd.gd = alloc_disk(16))){
    goto error_after_redister_blkdev;
    err=-EFAULT;
  }
  stackbd.gd->major = major_num;
  stackbd.gd->first_minor = 0;
  stackbd.gd->fops = &stackbd_ops;
  stackbd.gd->private_data = &stackbd;
  strcpy(stackbd.gd->disk_name, STACKBD_NAME_0);
  stackbd.gd->queue = stackbd.queue;
  add_disk(stackbd.gd);
  printk("rpg %s:stackbd: register done\n",__func__);
  if (stackbd_start(BACKUP_DISK) < 0){
    printk("rpg %s:Kernel call returned: %m",__func__);
    err= -1;
  }

  goto out;

error_after_redister_blkdev:
  unregister_blkdev(major_num, STACKBD_NAME);
error_after_alloc_queue:
  blk_cleanup_queue(stackbd.queue);
  printk("rpg[%s]: stackbd queue cleaned up\n",__func__);
out:
  return err;
}

void unregister_stackbd_device(int major_num)
{
  if (stackbd.is_active)
  {
    kthread_stop(stackbd.thread);
    blkdev_put(stackbd.bdev_raw, STACKBD_BDEV_MODE);
    bdput(stackbd. bdev_raw);
  }
  del_gendisk(stackbd.gd);
  put_disk(stackbd.gd);
  unregister_blkdev(major_num, STACKBD_NAME);
  blk_cleanup_queue(stackbd.queue);
}

void cleanup_stackbd_queue(void){
  blk_cleanup_queue(stackbd.queue);
}

