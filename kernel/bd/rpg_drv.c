/*
 * rpg, remote memory paging over RDMA
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
#include "radixtree.h"
#include "rdmabox.h"
#include "diskbox.h"
#include "rpg_mempool.h"
//#include "zrambox.h"
#include <linux/smp.h>

/*
#if MAX_SECTORS > RDMA_WR_BUF_LEN
#error MAX_SECTORS cannot be larger than RDMA_WR_BUF_LEN
#endif
*/

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
struct bio_set *io_bio_set;
#endif

#define DEBUG

#define DRV_NAME    "IS"
#define PFX     DRV_NAME ": "
#define DRV_VERSION "0.0"

MODULE_AUTHOR("Juhyun Bae");
MODULE_DESCRIPTION("RDMAbox, remote memory paging over RDMA");
MODULE_LICENSE("Dual BSD/GPL");
MODULE_VERSION(DRV_VERSION);

#define DEBUG_LOG if (1) printk

/* Globals */
int RDMABOX_major;
struct list_head g_RDMABOX_sessions;
struct mutex g_lock;
int submit_queues; // num of available cpu (also connections)
/*
static void msg_reset(struct rdmabox_msg *msg)
{
  memset(&msg->data_tbl, 0, sizeof(msg->data_tbl));
  atomic_set(&msg->ref_count,0);
}
*/
#if LINUX_VERSION_CODE < KERNEL_VERSION(3, 16, 0)
static struct blk_mq_hw_ctx *RDMABOX_alloc_hctx(struct blk_mq_reg *reg,
    unsigned int hctx_index)
{
  int b_size = DIV_ROUND_UP(reg->nr_hw_queues, nr_online_nodes); 
  int tip = (reg->nr_hw_queues % nr_online_nodes);
  int node = 0, i, n;
  struct blk_mq_hw_ctx * hctx;

#ifdef DEBUG
  //printk("rpg[%s]: hctx_index=%u, b_size=%d, tip=%d, nr_online_nodes=%d\n", __func__,
   //   hctx_index, b_size, tip, nr_online_nodes);
  //
#endif
  /*
   * Split submit queues evenly wrt to the number of nodes. If uneven,
   * fill the first buckets with one extra, until the rest is filled with
   * no extra.
   */
  for (i = 0, n = 1; i < hctx_index; i++, n++) {
    if (n % b_size == 0) {
      n = 0;
      node++;

      tip--;
      if (!tip)
	b_size = reg->nr_hw_queues / nr_online_nodes;
    }
  }

  /*
   * A node might not be online, therefore map the relative node id to the
   * real node id.
   */
  for_each_online_node(n) {
    if (!node)
      break;
    node--;
  }
  pr_debug("%s: n=%d\n", __func__, n);
  hctx = kzalloc_node(sizeof(struct blk_mq_hw_ctx), GFP_KERNEL, n);

  return hctx;
}

static void RDMABOX_free_hctx(struct blk_mq_hw_ctx *hctx, unsigned int hctx_index)
{
  //printk("rpg[%s]: %s called\n", __func__);
  //
  kfree(hctx);
}
#endif

static int RDMABOX_request(struct request *req, struct RDMABOX_queue *xq)
{
  int err=0;
  int cpu;
#if defined(SUPPORT_HYBRID_WRITE_SINGLE)
  unsigned int nr_seg = req->nr_phys_segments;
#endif
  //unsigned char char_cpu;
  //unsigned char delay;
  //size_t index;
  //struct rdma_ctx *ctx[NUM_REPLICA];
  //struct kernel_cb *replica_cb_list[NUM_REPLICA];
  //int swapspace_index;
  //unsigned long start_tmp, start_idx_tmp;  
  //struct timeval start, end;
  //unsigned long diff;
  //do_gettimeofday(&start);

  struct RDMABOX_session *RDMABOX_sess = xq->RDMABOX_sess;
  //struct RDMABOX_session *RDMABOX_sess = xq->RDMABOX_conn->RDMABOX_sess;
  //index = blk_rq_pos(req) >> SECTORS_PER_PAGE_SHIFT;
  //flags = zram_get_flag(index);

  //start_idx_tmp = blk_rq_pos(tmp) >> SECTORS_PER_PAGE_SHIFT; // 3
  //start_tmp = start_idx_tmp << PAGE_SHIFT;
  //swapspace_index = start_tmp >> ONE_GB_SHIFT;

/*
  struct rdmabox_msg *msg = req->special;
  msg_reset(msg);
  msg->req = req;
*/
  switch (rq_data_dir(req)) {
    case READ:
#if defined(USE_SGE_READ) || defined(USE_SINGLE_IO_PRE_MR_READ)
      cpu = get_cpu();
      //printk("rpg[%s]: READ index=%lu \n", __func__,index);
      err = request_read(RDMABOX_sess,req,cpu);
      if(err){
           //printk("brd[%s]: Remote miss. index=%zu \n",__func__,index);
           err = radix_read(RDMABOX_sess,req);
           if(err){
               //printk("brd[%s]: local miss. index=%zu \n",__func__,index);
#ifdef DISKBACKUP
               // read from disk
               //printk("brd[%s]: Remote read fail. Read from disk \n",__func__);
               RDMABOX_mq_request_stackbd(req);
#else
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
               blk_mq_end_request(req, 0);
#else
               blk_mq_end_io(req, 0);
#endif
#endif// DISKBACKUP
	   }
      }
      put_cpu();
#elif defined(SUPPORT_HYBRID_READ)
retry_read:
      cpu = get_cpu();
      err = put_read_req_item(req);
      if(err){
           printk("rpg_drv: retry put read req \n");
	   goto retry_read;
      }
      //udelay(1);
      //get_random_bytes(&char_cpu, sizeof(char_cpu));
      //char_cpu%=submit_queues;
      hybrid_batch_read_fn(RDMABOX_sess, cpu);
      put_cpu();
#elif defined(USE_DOORBELL_READ) || defined(USE_DOORBELL_READ_SGE)
retry_read:
      cpu = get_cpu();
      err = put_read_req_item(req);
      if(err){
         printk("rpg_drv: retry put write req \n");
         goto retry_read;
      }
      doorbell_batch_read_fn(RDMABOX_sess, cpu);
      put_cpu();

#elif defined(NEW_HYBRID_READ)
retry_read:
      cpu = get_cpu();
      err = put_read_req_item(req);
      if(err){
         printk("rpg_drv: retry put write req \n");
         goto retry_read;
      }
      new_hybrid_batch_read_fn(RDMABOX_sess, cpu);
      put_cpu();

#else // BATCH
retry_read:
      cpu = get_cpu();
      err = put_read_req_item(req);
      if(err){
           printk("rpg_drv: retry put read req \n");
	   goto retry_read;
      }
      //udelay(1);
      //get_random_bytes(&char_cpu, sizeof(char_cpu));
      //char_cpu%=submit_queues;
      batch_read_fn(RDMABOX_sess, cpu);
      put_cpu();
#endif
      break;

    case WRITE:
#if defined(USE_SGE_WRITE)
    //printk("rpg[%s]: WRITE index=%lu \n", __func__,index);
      cpu = get_cpu();

      //err = radix_write(RDMABOX_sess,req);
      // SGE doesn't use MR. locking on ctx is fast. So using random index is overkill.
      request_write_sge(RDMABOX_sess,req,cpu);
      put_cpu();
#elif defined(USE_SINGLE_IO_PRE_MR_WRITE)
      //printk("rpg[%s]: WRITE index=%lu \n", __func__,index);
      cpu = get_cpu();

      //err = radix_write(RDMABOX_sess,req);
      //get_random_bytes(&char_cpu, sizeof(char_cpu));
      //char_cpu%=submit_queues;
      //err = request_write(RDMABOX_sess,req,char_cpu);
      request_write(RDMABOX_sess,req,cpu);
      put_cpu();
#elif defined(SUPPORT_HYBRID_WRITE_SINGLE)
      cpu = get_cpu();
      
      if(nr_seg > MAX_SGE){
          //printk("rpg[%s]: call request_write index=%lu nr_seg:%d \n", __func__,index, nr_seg);
          request_write(RDMABOX_sess, req, cpu);
      }else{
          //printk("rpg[%s]: call request_write_sge index=%lu nr_seg:%d \n", __func__,index, nr_seg);
          request_write_sge(RDMABOX_sess,req,cpu);
      }
      put_cpu();
#elif defined(SUPPORT_HYBRID_WRITE)
retry_write:
      cpu = get_cpu();
/*
      err = pre_check_write(RDMABOX_sess, req, cpu);
      if(err){
         return 0;
      }
*/
      err = put_req_item(req);
      if(err){
         printk("rpg_drv: retry put write req \n");
         goto retry_write;
      }
      //prepare_write(req, cpu)
      //get_random_bytes(&char_cpu, sizeof(char_cpu));
      //char_cpu%=submit_queues;
      //hybrid_batch_write_fn(RDMABOX_sess, char_cpu);
      //udelay(1);
      hybrid_batch_write_fn(RDMABOX_sess, cpu);
      put_cpu();
#elif defined(USE_DOORBELL_WRITE) || defined(USE_DOORBELL_WRITE_SGE)
retry_write:
      cpu = get_cpu();
      err = put_req_item(req);
      if(err){
         printk("rpg_drv: retry put write req \n");
         goto retry_write;
      }
      doorbell_batch_write_fn(RDMABOX_sess, cpu);
      put_cpu();

#elif defined(NEW_HYBRID_WRITE)
retry_write:
      cpu = get_cpu();

      err = pre_check_write(RDMABOX_sess, req, cpu);
      if(err){
         return 0;
      }

      err = put_req_item(req);
      if(err){
         printk("rpg_drv: retry put write req \n");
         goto retry_write;
      }
      new_hybrid_batch_write_fn(RDMABOX_sess, cpu);
      put_cpu();

#else //BATCH
retry_write:
      cpu = get_cpu();
/*
      err = pre_check_write(RDMABOX_sess, req, cpu);
      if(err){
         return 0;
      }
*/
      err = put_req_item(req);
      if(err){
         printk("rpg_drv: retry put write req \n");
         goto retry_write;
      }
      //prepare_write(req, cpu)
      //get_random_bytes(&char_cpu, sizeof(char_cpu));
      //char_cpu%=submit_queues;
      //batch_write_fn(RDMABOX_sess, char_cpu);
      //udelay(1);
      batch_write_fn(RDMABOX_sess, cpu);
      put_cpu();
#endif
      break;
  }//switch
  return 0;
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 19, 0)
static int RDMABOX_queue_rq(struct blk_mq_hw_ctx *hctx, const struct blk_mq_queue_data *bd)
#elif LINUX_VERSION_CODE == KERNEL_VERSION(3, 18, 0)
static int RDMABOX_queue_rq(struct blk_mq_hw_ctx *hctx, struct request *rq, bool last)
#else
static int RDMABOX_queue_rq(struct blk_mq_hw_ctx *hctx, struct request *rq)
#endif
{
  struct RDMABOX_queue *RDMABOX_q;
  int err;
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 19, 0)
  struct request *rq = bd->rq;
#endif

  RDMABOX_q = hctx->driver_data; //get the queue from the hctx
  err = RDMABOX_request(rq, RDMABOX_q);

  if (unlikely(err)) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 15, 0)
    rq->errors = -EIO;
    return BLK_MQ_RQ_QUEUE_ERROR;
#else
    return BLK_STS_TIMEOUT;
#endif
  }

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
  blk_mq_start_request(rq);
#endif

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 15, 0)
  return BLK_STS_OK;
#else
  return BLK_MQ_RQ_QUEUE_OK;
#endif
}

// connect hctx with IS-file, IS-conn, and queue
static int RDMABOX_init_hctx(struct blk_mq_hw_ctx *hctx, void *data,
    unsigned int index)
{
  struct RDMABOX_file *xdev = data;
  struct RDMABOX_queue *xq;

  xq = &xdev->queues[index];
#ifdef DEBUG
  //printk("rpg[%s]: called index=%u xq=%p\n", __func__, index, xq);
  //
#endif
  //xq->RDMABOX_conn = xdev->RDMABOX_conns[index];
  xq->RDMABOX_sess = xdev->RDMABOX_sess;
  xq->xdev = xdev;
  xq->queue_depth = xdev->queue_depth;
  hctx->driver_data = xq;

  return 0;
}

static struct blk_mq_ops RDMABOX_mq_ops = {
  .queue_rq       = RDMABOX_queue_rq,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 9, 0)
  .map_queues      = blk_mq_map_queues,  
#else
  .map_queue      = blk_mq_map_queue,  
#endif
  .init_hctx	= RDMABOX_init_hctx,
#if LINUX_VERSION_CODE < KERNEL_VERSION(3, 16, 0)
  .alloc_hctx	= RDMABOX_alloc_hctx,
  .free_hctx	= RDMABOX_free_hctx,
#endif
};

#if LINUX_VERSION_CODE < KERNEL_VERSION(3, 16, 0)
static struct blk_mq_reg RDMABOX_mq_reg = {
  .ops		= &RDMABOX_mq_ops,
  //.cmd_size	= sizeof(struct rdmabox_msg),
  .cmd_size	= 0,
  .flags		= BLK_MQ_F_SHOULD_MERGE,
  .numa_node	= NUMA_NO_NODE,
  .queue_depth	= RDMABOX_QUEUE_DEPTH,
};
#endif

int RDMABOX_setup_queues(struct RDMABOX_file *xdev)
{
  pr_debug("%s called\n", __func__);
  xdev->queues = kzalloc(submit_queues * sizeof(*xdev->queues),
      GFP_KERNEL);
  if (!xdev->queues)
    return -ENOMEM;

  return 0;
}

static int RDMABOX_open(struct block_device *bd, fmode_t mode)
{
  pr_debug("%s called\n", __func__);
  return 0;
}

static void RDMABOX_release(struct gendisk *gd, fmode_t mode)
{
  pr_debug("%s called\n", __func__);
}

static int RDMABOX_media_changed(struct gendisk *gd)
{
  pr_debug("%s called\n", __func__);
  return 0;
}

static int RDMABOX_revalidate(struct gendisk *gd)
{
  pr_debug("%s called\n", __func__);
  return 0;
}

static int RDMABOX_ioctl(struct block_device *bd, fmode_t mode,
    unsigned cmd, unsigned long arg)
{
  pr_debug("%s called\n", __func__);
  return -ENOTTY;
}

// bind to RDMABOX_file in RDMABOX_register_block_device
static struct block_device_operations RDMABOX_ops = {
  .owner           = THIS_MODULE,
  .open 	         = RDMABOX_open,
  .release 	 = RDMABOX_release,
  .media_changed   = RDMABOX_media_changed,
  .revalidate_disk = RDMABOX_revalidate,
  .ioctl	         = RDMABOX_ioctl
};

void RDMABOX_destroy_queues(struct RDMABOX_file *xdev)
{
  printk("rpg[%s]:\n", __func__);
  
  kfree(xdev->queues);
}

int RDMABOX_register_block_device(struct RDMABOX_file *RDMABOX_file)
{
  //sector_t size = RDMABOX_file->stbuf.st_size;
  u64 size = RDMABOX_file->stbuf.st_size;
  int page_size = PAGE_SIZE;
  int err = 0;

#ifdef DEBUG
  printk("rpg[%s]: register rpg block size=%llu \n", __func__,(long long unsigned int)size);
  
#endif

  // mempool init
  err = mempool_init();
  if (err)
    goto out;

  // init zram
  //err = zram_init(size);
  err = brd_init(size);
  if (err)
    goto out;
 
#ifdef DISKBACKUP
  // register stackbd
  err = register_stackbd_device(major_num);
  if (err)
    goto out;
#endif

  // register RDMABOX_file
  RDMABOX_file->major = RDMABOX_major;

  // set device params 
#if LINUX_VERSION_CODE < KERNEL_VERSION(3, 16, 0)
  RDMABOX_mq_reg.nr_hw_queues = submit_queues;
  RDMABOX_file->queue = blk_mq_init_queue(&RDMABOX_mq_reg, RDMABOX_file);  // RDMABOX_mq_req was defined above
#else
  RDMABOX_file->tag_set.ops = &RDMABOX_mq_ops;
  RDMABOX_file->tag_set.nr_hw_queues = submit_queues;
  RDMABOX_file->tag_set.queue_depth = RDMABOX_QUEUE_DEPTH;
  RDMABOX_file->tag_set.numa_node = NUMA_NO_NODE;
  //RDMABOX_file->tag_set.cmd_size	= sizeof(struct rdmabox_msg);
  RDMABOX_file->tag_set.cmd_size	= 0;
  RDMABOX_file->tag_set.flags = BLK_MQ_F_SHOULD_MERGE;
  RDMABOX_file->tag_set.driver_data = RDMABOX_file;

  err = blk_mq_alloc_tag_set(&RDMABOX_file->tag_set);
  if (err)
    goto out;

  RDMABOX_file->queue = blk_mq_init_queue(&RDMABOX_file->tag_set);
#endif
  if (IS_ERR(RDMABOX_file->queue)) {
    printk("rpg[%s]: Failed to allocate blk queue ret=%ld\n",
	__func__, PTR_ERR(RDMABOX_file->queue));
    
    err = PTR_ERR(RDMABOX_file->queue);
    goto blk_mq_init;
  }

  RDMABOX_file->queue->queuedata = RDMABOX_file;
  queue_flag_set_unlocked(QUEUE_FLAG_NONROT, RDMABOX_file->queue);
  queue_flag_clear_unlocked(QUEUE_FLAG_ADD_RANDOM, RDMABOX_file->queue);

  RDMABOX_file->disk = alloc_disk_node(1, NUMA_NO_NODE);
  if (!RDMABOX_file->disk) {
    printk("rpg[%s]: Failed to allocate disk node\n", __func__);
    
    err = -ENOMEM;
    goto alloc_disk;
  }

  // device setting info, kernel may make swap based on this info
  RDMABOX_file->disk->major = RDMABOX_file->major;
  RDMABOX_file->disk->first_minor = RDMABOX_file->index;
  RDMABOX_file->disk->fops = &RDMABOX_ops;	// pay attention to RDMABOX_ops
  RDMABOX_file->disk->queue = RDMABOX_file->queue;
  RDMABOX_file->disk->private_data = RDMABOX_file;
  blk_queue_logical_block_size(RDMABOX_file->queue, RDMABOX_SECT_SIZE); //block size = 512
  blk_queue_physical_block_size(RDMABOX_file->queue, RDMABOX_SECT_SIZE);
  sector_div(page_size, RDMABOX_SECT_SIZE);
  // TODO : set system's max queue automatically.
  blk_queue_max_hw_sectors(RDMABOX_file->queue, page_size * MAX_SECTORS);
  sector_div(size, RDMABOX_SECT_SIZE);
  set_capacity(RDMABOX_file->disk, size);  // size is in remote file state->size, add size info into block device
  sscanf(RDMABOX_file->dev_name, "%s", RDMABOX_file->disk->disk_name);

  printk("rpg[%s]: dev_name %s\n", __func__, RDMABOX_file->dev_name);

  add_disk(RDMABOX_file->disk);

  printk("rpg[%s]: init done\n",__func__);

  goto out;

alloc_disk:
  blk_cleanup_queue(RDMABOX_file->queue);
blk_mq_init:
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 16, 0)
  blk_mq_free_tag_set(&RDMABOX_file->tag_set);
#endif
  return err;

error_after_redister_blkdev:
  unregister_blkdev(major_num, STACKBD_NAME); 
error_after_alloc_queue:
  cleanup_stackbd_queue();
  printk("rpg[%s]: stackbd queue cleaned up\n",__func__);
  
out:
  return err;
}

void RDMABOX_unregister_block_device(struct RDMABOX_file *RDMABOX_file)
{
  //zram_destroy();

  del_gendisk(RDMABOX_file->disk);
  blk_cleanup_queue(RDMABOX_file->queue);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 16, 0)
  blk_mq_free_tag_set(&RDMABOX_file->tag_set);
#endif
  put_disk(RDMABOX_file->disk);

  printk("rpg[%s]: stackbd: exit\n", __func__);
  

  unregister_stackbd_device(major_num);
}

static int __init RDMABOX_init_module(void)
{
  if (RDMABOX_create_configfs_files())
    return 1;

  printk("rdmabox[%s]: nr_cpu_ids=%d, num_online_cpus=%d\n", __func__, nr_cpu_ids, num_online_cpus());
  

  submit_queues = num_online_cpus();

  RDMABOX_major = register_blkdev(0, "rpg");
  if (RDMABOX_major < 0)
    return RDMABOX_major;

  mutex_init(&g_lock);
  INIT_LIST_HEAD(&g_RDMABOX_sessions);


  return 0;
}

// module function
static void __exit RDMABOX_cleanup_module(void)
{
  struct RDMABOX_session *RDMABOX_session, *tmp;

  list_for_each_entry_safe(RDMABOX_session, tmp, &g_RDMABOX_sessions, list) {
    RDMABOX_session_destroy(RDMABOX_session);
  }

  RDMABOX_destroy_configfs_files();

  unregister_blkdev(RDMABOX_major, "rpg");
}

module_init(RDMABOX_init_module);
module_exit(RDMABOX_cleanup_module);
