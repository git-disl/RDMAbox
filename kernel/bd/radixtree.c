/*
 i Ram backed block device driver.
 *
 * Copyright (C) 2007 Nick Piggin
 * Copyright (C) 2007 Novell Inc.
 *
 * Parts derived from drivers/block/rd.c, and drivers/block/loop.c, copyright
 * of their respective owners.
 * drivers/block/brd.c
 */

#include "debug.h"

#include "rpg_drv.h"
#include "radixtree.h"
#include "rpg_mempool.h"
#include "rdmabox.h"
#include "diskbox.h"

// TODO ALL : brd type struct or pointer ?

int test_flag(struct tree_entry *entry,enum entry_flags flag)
{
        return entry->flags & BIT(flag);
}
        
int set_flag(struct tree_entry *entry,enum entry_flags flag)
{
        entry->flags |= BIT(flag);
}
        
int clear_flag(struct tree_entry *entry,enum entry_flags flag)
{
        entry->flags &= ~BIT(flag);
}

struct tree_entry *brd_lookup_page(struct brd_device *brd, size_t index)
{
	struct tree_entry *entry;

	rcu_read_lock();
	entry = radix_tree_lookup(&brd->brd_pages, index);
	rcu_read_unlock();

	return entry; 
}

struct tree_entry *brd_lookup(size_t index)
{
    return brd_lookup_page(&brd,index);
}

struct tree_entry* brd_alloc_entry(struct brd_device *brd, size_t index)
{
	struct tree_entry *entry;
	unsigned long flags;

        entry = (struct tree_entry *)kmalloc(sizeof(struct tree_entry),GFP_ATOMIC); 
        if(!entry){
	    //printk("brd[%s]: allocate entry fail \n", __func__);
	    return NULL;
        }
        //get_cpu(); 
        entry->page = rpg_alloc();
	if(!entry->page){
	    //printk("brd[%s]: allocate rpg_alloc fail \n", __func__);
            put_cpu();
            kfree(entry);
	    return NULL;
	}
        // TODO : add data integrity check later by using this
	//entry->index = index;
	entry->len=RDMABOX_PAGE_SIZE;
	entry->flags=0;
        //put_cpu();

	return entry;
}

struct tree_entry* brd_alloc(size_t index)
{
    return brd_alloc_entry(&brd, index);
}

struct tree_entry* brd_insert_page(struct brd_device *brd, struct tree_entry *entry, size_t index)
{
        unsigned long flags;

        spin_lock_irqsave(&brd->brd_lock, flags);
	entry = radix_tree_lookup(&brd->brd_pages, index);
        spin_unlock_irqrestore(&brd->brd_lock, flags);
        if(!entry){
            entry = brd_alloc_entry(brd, index);
            if(!entry){
                //printk("brd[%s]: brd alloc entry fail \n", __func__);
                //panic(" brd alloc fail");
	        return NULL;
            }
	    atomic_set(&entry->ref_count,0);
        }else{
            //printk("brd[%s]: entry exists. It is updating transaction. index=%lu \n", __func__,index);
            if(!entry->page){
                //printk("brd[%s]: entry exists but page is null. rpg_alloc() here \n", __func__);
                entry->page = rpg_alloc();
		if(!entry->page)
		    return NULL;
            }
	    atomic_inc(&entry->ref_count);
            set_flag(entry,UPDATING);
	    return entry;
        }
       
       	if (radix_tree_preload(GFP_NOIO)) {
		rpg_free(entry->page);
	        kfree(entry);
		printk("brd[%s]: radix_tree_preload fail \n", __func__);
		return NULL;
	}

        spin_lock_irqsave(&brd->brd_lock, flags);
	if (radix_tree_insert(&brd->brd_pages, index, entry)) {
                //spin_unlock_irqrestore(&brd->brd_lock, flags);
		printk("brd[%s]: radix_tree_insert fail. entry exists in radix_tree. \n", __func__);
		panic("radix_tree_insert fail");
	}
        spin_unlock_irqrestore(&brd->brd_lock, flags);
	radix_tree_preload_end();

	return entry;
}

struct tree_entry* brd_insert(struct tree_entry *entry, size_t index)
{
    return brd_insert_page(&brd, entry, index);
}

void brd_free_entry(struct brd_device *brd, size_t index)
{
	struct tree_entry *entry;
	unsigned long flags;

	spin_lock_irqsave(&brd->brd_lock, flags);
	entry = radix_tree_delete(&brd->brd_pages, index);
	spin_unlock_irqrestore(&brd->brd_lock, flags);
	if (entry){
		rpg_free(entry->page);
		kfree(entry);
	}
}

void brd_free(size_t index)
{
	brd_free_entry(&brd, index);
}

struct page * brd_remove_page(struct brd_device *brd, size_t index)
{
	struct page *page;
	struct tree_entry *entry;
	unsigned long flags;

        spin_lock_irqsave(&brd->brd_lock, flags);
	entry = radix_tree_lookup(&brd->brd_pages, index);
	if(!entry){
	    //panic("brd_remove_page : entry is null");
	    printk("brd_remove_page : entry is null\n");
            spin_unlock_irqrestore(&brd->brd_lock,flags);
	    return NULL;
	}
/*
	if(entry->ref_count > 0){
                printk("brd[%s]: page is updating. skip this page. index=%zu \n", __func__,index);
		return NULL;
	}
*/
	page = entry->page;
        entry->page = NULL;
        entry->len = 0;
        spin_unlock_irqrestore(&brd->brd_lock,flags);

	return page;
}

struct page * brd_remove(size_t index)
{
	return brd_remove_page(&brd, index);
}
/*
int brd_update_page(struct brd_device *brd, char *buffer, struct local_page_list *tmp)
{
	unsigned long flags;
        unsigned char *cmem;
	size_t index;
        int i;

	index = tmp->start_index;
        for (i=0; i < tmp->len;i++){
            struct tree_entry *entry;

            entry = brd_lookup_page(brd, index);
            if(!entry){
	        return 1;
            }else{
              if (!entry->page){
                printk("brd[%s]: page is not stored index=%zu \n", __func__,index);
                entry->page = rpg_alloc();
                if (!entry->page)
		    return 1;
              }else{
                printk("brd[%s]: page is already stored. Skip. index=%zu \n", __func__,index);
                return 1;
              }
            }

            spin_lock_irqsave(&brd->brd_lock, flags);
            cmem = kmap_atomic(entry->page);
            memcpy(cmem, buffer + (i*RDMABOX_PAGE_SIZE), RDMABOX_PAGE_SIZE);
            kunmap_atomic(cmem);
            entry->len = RDMABOX_PAGE_SIZE;
	    spin_unlock_irqrestore(&brd->brd_lock, flags);

            printk("brd[%s]: proactive read page is done. index=%zu \n", __func__,index);

	    index++;
	}

        return 0;
}

int brd_update(char *buffer, struct local_page_list *tmp)
{
    return brd_update_page(&brd, buffer, tmp);
}
*/
/*
struct tree_entry * brd_start_update_page(struct brd_device *brd, struct tree_entry *tmp)
{
	unsigned long flags;

        entry = brd_lookup_page(brd, index);
        if(!entry){
	    return NULL;
        }else{
          if (!entry->page){
            printk("cpu(%d)rdmabox[%s]: page is not stored index=%zu \n",get_cpu(), __func__,index);
            entry->page = rpg_alloc();
          }else{
            printk("cpu(%d)rdmabox[%s]: page is already stored in Local. Skip. index=%zu \n",get_cpu(), __func__,index);
            break;
          }
        }

	spin_lock_irqsave(&brd->brd_lock, flags);
        brd->state = UPDATE_START;
	wait_event_interruptible(brd->sem, brd->state == UPDATE_DONE);
        tmp->page = page;
	tmp->len = len;

	spin_unlock_irqrestore(&brd->brd_lock, flags);
        return tmp;
}

struct tree_entry * brd_start_update(struct tree_entry *tmp)
{
    return brd_start_update_page(&brd, tmp);
}

void brd_update_done()
{
    &brd->state=UPDATE_DONE;
    wake_up_interruptible(&brd->sem);
}
*/

/*
static void brd_zero_page(struct brd_device *brd, size_t index)
{
	struct page *page;

	page = brd_lookup_page(brd, index);
	if (page)
		clear_highpage(page);
}
*/

/*
 * Free all backing store pages and radix tree. This must only be called when
 * there are no other users of the device.
 */
#define FREE_BATCH 16
static void brd_free_pages(struct brd_device *brd)
{
	unsigned long pos = 0;
	struct tree_entry *entries[FREE_BATCH];
	int nr_entries;

	do {
		int i;

		nr_entries = radix_tree_gang_lookup(&brd->brd_pages,
				(void **)entries, pos, FREE_BATCH);

		for (i = 0; i < nr_entries; i++) {
			void *ret;

			BUG_ON(entries[i]->page->index < pos);
			pos = entries[i]->page->index;
			ret = radix_tree_delete(&brd->brd_pages, pos);
			BUG_ON(!ret || ret != entries[i]);
			// real delete
			__free_page(entries[i]->page);
			kfree(entries[i]);
		}

		pos++;

		/*
		 * This assumes radix_tree_gang_lookup always returns as
		 * many pages as possible. If the radix-tree code changes,
		 * so will this have to.
		 */
	} while (nr_entries == FREE_BATCH);
}
/*
static void discard_from_brd(struct brd_device *brd, size_t index)
{
	
	 // Don't want to actually discard pages here because
	 // re-allocating the pages can result in writeback
	 // deadlocks under heavy load.
	  // 
	if (0)
		brd_free_page(brd, index);
	else
		brd_zero_page(brd, index);
}
*/

/*
struct bio * create_bio_copy(struct bio *src)
{
   int	status;
   struct bio *dst;

	//Allocate a memory for a new 'bio' structure 
	if ( !(dst = bio_clone(src, GFP_KERNEL)) )
		{
		printk("bio_clone_bioset() -> NULL\n");

		return NULL;
		}

	if ( status = bio_alloc_pages(dst , GFP_KERNEL))
		{
		printk("bio_alloc_pages() -> %d\n", status);

		return NULL;
		}
  return dst;
}
*/
int write_to_brd(struct brd_device *brd, struct request *req, struct RDMABOX_session *RDMABOX_sess)
{
  int ret;
  size_t start_index;
  size_t i,j;
  unsigned char *src, *cmem;
  unsigned char *buffer;
  struct bio *tmp = req->bio;
  struct local_page_list *tmp_item;
  int cpu;

#ifdef DISKBACKUP
  struct bio *cloned_bio;
  cloned_bio = create_bio_copy(req->bio);
  if (unlikely(!cloned_bio)) {
    printk("brd[%s]: fail to clone bio \n",__func__);
    return 1;
  }
#endif
#ifdef LAT_DEBUG
  struct timeval start, end;
  unsigned long diff;
  do_gettimeofday(&start);
#endif

  if (unlikely(!brd->init_done)) {
    printk("brd[%s]: brd not init done yet \n",__func__);
    return 1;
  }
  start_index = blk_rq_pos(req) >> SECTORS_PER_PAGE_SHIFT; // 3

  //printk("brd[%s]: Write Local start_index=%u nr_phys_segments=%d \n", __func__,start_index,req->nr_phys_segments);
  cpu = get_cpu();
repeat:
  tmp_item = get_free_item();
  if(unlikely(!tmp_item)){
	//printk("brd[%s]: Fail to allocate list item. retry \n", __func__);
	goto repeat;
  }
  tmp_item->start_index = start_index;
  tmp_item->len = req->nr_phys_segments;
#ifdef DISKBACKUP
  tmp_item->cloned_bio = cloned_bio;
#endif

  for (i=0; i < req->nr_phys_segments;i++){
        struct tree_entry *entry;
        entry = brd_insert_page(brd, entry, start_index+i);
	tmp_item->batch_list[i] = entry;
  }

#ifdef LAT_DEBUG
  do_gettimeofday(&end);
  diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
  printk("WR radix insert %ld usec\n",diff);
  do_gettimeofday(&start);
#endif

  for (i=0; i < req->nr_phys_segments;i++){
    struct tree_entry *entry;
    entry = tmp_item->batch_list[i];
    buffer = bio_data(tmp);

    cmem = kmap_atomic(entry->page);
    memcpy(cmem, buffer, RDMABOX_PAGE_SIZE);
    kunmap_atomic(cmem);

    entry->len = RDMABOX_PAGE_SIZE;

    tmp = tmp->bi_next;
  }

#ifdef LAT_DEBUG
  do_gettimeofday(&end);
  diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
  printk("WR copy %ld usec\n",diff);
  do_gettimeofday(&start);
#endif

  // put index sending list here
  // later, it will be moved to recliamable list by remote sender thread
send_again:
  ret = put_sending_index(tmp_item);
  if(ret){
	printk("brd[%s]: Fail to put sending index. retry \n", __func__);
	goto send_again;
  }

#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
  blk_mq_end_request(req, 0);
#else
  blk_mq_end_io(req, 0);
#endif

  remote_sender_fn(RDMABOX_sess, cpu);

#ifdef LAT_DEBUG
  do_gettimeofday(&end);
  diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
  printk("WR enqueue %ld usec\n",diff);
#endif

  put_cpu();
  return 0;
}

// TODO : remove xq
//int radix_write(struct request *req, struct RDMABOX_queue *xq)
int radix_write(struct RDMABOX_session *RDMABOX_sess, struct request *req)
{
   //return write_to_brd(&brd, req, xq);
   return write_to_brd(&brd, req, RDMABOX_sess);
}

// TODO : batch proactive read
int read_from_brd(struct brd_device *brd, struct request *req, struct RDMABOX_session *RDMABOX_sess)
{
  int i, ret;
  size_t index;
  struct tree_entry *entry;
  unsigned char *cmem;
  struct local_page_list *tmp_item;
  int cpu=get_cpu();

  if (unlikely(!brd->init_done)) {
    printk("brd[%s]: brd not init done yet \n", __func__);
    put_cpu();
    return 1;
  }

#ifdef LAT_DEBUG
  struct timeval start, end;
  unsigned long diff;
  do_gettimeofday(&start);
#endif

  index = blk_rq_pos(req) >> SECTORS_PER_PAGE_SHIFT;
  //printk("cpu(%d)brd[%s]: Read Local start_index=%u \n",cpu, __func__,index);

  //rcu_read_lock();
  entry = radix_tree_lookup(&brd->brd_pages, index);
  if (entry && entry->page) {
      cmem = kmap_atomic(entry->page);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
      memcpy(bio_data(req->bio), cmem, entry->len);
#else
      memcpy(req->buffer, cmem, entry->len);
#endif
      kunmap_atomic(cmem);
      //rcu_read_unlock();

      //printk("cpu(%d)brd[%s]: Local hit ! index=%u \n",cpu, __func__,index);
#ifdef LAT_DEBUG
      do_gettimeofday(&end);
      diff=(end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
      printk("RD local radix %ld usec\n",diff);
#endif
      goto endio;
  } 
  //rcu_read_unlock();
/*
   //printk("cpu(%d)brd[%s]: Local miss. index=%zu \n",cpu, __func__,index);

   // if local miss, request remote read
  //ret = request_read(RDMABOX_sess,req,tmp_item);
  ret = request_read(RDMABOX_sess,req);
  if(ret){
       //printk("cpu(%d)brd[%s]: Remote miss. index=%zu \n",cpu, __func__,index);
#ifdef DISKBACKUP
       // read from disk
       //printk("brd[%s]: Remote read fail. Read from disk \n",__func__);
       RDMABOX_mq_request_stackbd(req);
#else
       goto endio;
#endif
  }
*/

  put_cpu();
  return 1;

endio:
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 18, 0)
  blk_mq_end_request(req, 0);
#else
  blk_mq_end_io(req, 0);
#endif

out:
  put_cpu();
  return 0;
}

//int radix_read(struct request *req, struct RDMABOX_queue *xq)
int radix_read(struct RDMABOX_session *RDMABOX_sess, struct request *req)
{
   //return read_from_brd(&brd, req, xq);
   return read_from_brd(&brd, req, RDMABOX_sess);

}

int brd_init(u64 size)
{
	//struct brd_device *brd;

        printk("brd[%s]: brd init start \n", __func__);
/*
	brd = kzalloc(sizeof(*brd), GFP_KERNEL);
	if (!brd)
		goto out;
*/
        brd.init_done = 0;

	spin_lock_init(&brd.brd_lock);
	//spin_lock_init(&brd.brd_ref_count_lock);
	INIT_RADIX_TREE(&brd.brd_pages, GFP_ATOMIC);

        brd.init_done = 1;

        printk("brd[%s]: brd init done \n", __func__);
	return 0;

//out:
	return -ENOMEM;
}

void brd_destroy(struct brd_device *brd)
{
	brd_free_pages(brd);
	kfree(brd);
}
