/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
 *
 *  linux/mm/mempool.c
 *
 *  memory buffer pool support. Such pools are mostly used
 *  for guaranteed, deadlock-free memory allocations during
 *  extreme VM load.
 *
 *  started by Ingo Molnar, Copyright (C) 2001
 */

#include <linux/mm.h>
#include <linux/slab.h>
#include <linux/module.h>
#include <linux/blkdev.h>
#include <linux/writeback.h>
#include <linux/sched.h>
#include <linux/sys.h>
#include <linux/kernel.h>

#include "debug.h"
#include "rpg_drv.h"
#include "rpg_mempool.h"
#include "radixtree.h"
#include "alf_queue.h"

// sysinfo for freemem info
struct sysinfo info;
/*
struct rdmabox_msg* get_free_msg()
{
   struct rdmabox_msg *tmp;
   if (alf_queue_empty(rpg_page_pool.msg_freepool_list)){
        tmp = (struct rdmabox_msg *)vmalloc(sizeof(struct rdmabox_msg));
	if(!tmp){
	    printk("mempool[%s]: Fail to allocate free item \n", __func__);
            panic("fail to allocate free item");
	}
	goto out;
   }

   if (alf_mc_dequeue(rpg_page_pool.msg_freepool_list, (void **)&tmp, 1) == 0){
	return NULL;
     }
    //printk("mempool[%s]:Get free item free_item_count=%u \n",__func__,
    //         alf_queue_count(rpg_page_pool.msg_freepool_list));
out:
    return tmp;
}

int put_free_msg(struct rdmabox_msg *tmp)
{  
    if (alf_mp_enqueue(rpg_page_pool.msg_freepool_list, (void **)&tmp, 1) != 1){
	printk("mempool[%s]: Fail to put free_item \n", __func__);
	vfree(tmp);
	return 1;
    }
    //printk("mempool[%s]:Put free item free_item_count=%u \n",__func__,
    //         alf_queue_count(rpg_page_pool.msg_freepool_list));

    return 0;
}
*/
struct local_page_list* get_free_item()
{
   struct local_page_list *tmp;
/*
   if (alf_queue_empty(rpg_page_pool.freepool_list)){
        //tmp = (struct local_page_list *)vmalloc(sizeof(struct local_page_list));
*/
        tmp = (struct local_page_list *)kmalloc(sizeof(struct local_page_list),GFP_ATOMIC);
	if(!tmp){
	    printk("mempool[%s]: Fail to allocate free item \n", __func__);
            panic("fail to allocate free item");
	}
/*
	goto out;
   }

   if (alf_mc_dequeue(rpg_page_pool.freepool_list, (void **)&tmp, 1) == 0){
	return NULL;
     }
    //printk("mempool[%s]:Get free item free_item_count=%u \n",__func__,
    //         alf_queue_count(rpg_page_pool.freepool_list));
out:
*/
    return tmp;
}

int put_free_item(struct local_page_list *tmp)
{  
/*
    if (alf_mp_enqueue(rpg_page_pool.freepool_list, (void **)&tmp, 1) != 1){
	printk("mempool[%s]: Fail to put free_item \n", __func__);
	//vfree(tmp);
*/
	kfree(tmp);
/*
	return 1;
    }
*/
    //printk("mempool[%s]:Put free item free_item_count=%u \n",__func__,
    //         alf_queue_count(rpg_page_pool.freepool_list));
    //

    return 0;
}

int put_item(struct alf_queue * list, void *item)
{
    if (alf_mp_enqueue(list, (void **)&item, 1) != 1){
	printk("mempool[%s]: Fail to put sending index \n", __func__);
	return 1;
    }
    //printk("mempool[%s]:Put item count=%u \n",__func__,alf_queue_count(list));
    
    return 0;
}

int get_item(struct alf_queue * list, void **item)
{
    if(unlikely(rpg_page_pool.init_done == 0)){
	return 1;
    }

    if (alf_queue_empty(list)){
	return 1;
    }

    if (alf_mc_dequeue(list, (void **)item, 1) == 0){
	return 1;
    }

    //printk("mempool[%s]:Get item count=%u \n", __func__,alf_queue_count(list));
    
    return 0;
}

int put_read_req_item(void *tmp)
{
    if (alf_mp_enqueue(rpg_page_pool.read_req_list, (void **)&tmp, 1) != 1){
	printk("mempool[%s]: Fail to put req item \n",__func__);
	return 1;
    }
    //printk("mempool[%s]:put item count=%u \n", __func__,alf_queue_count(rpg_page_pool.req_list));
    return 0;
}

int get_read_req_item(void **tmp)
{
    if (alf_queue_empty(rpg_page_pool.read_req_list)){
	return 1;
    }

    if (alf_mc_dequeue(rpg_page_pool.read_req_list, (void **)tmp, 1) == 0){
	return 1;
    }
    //printk("mempool[%s]:get item count=%u \n", __func__,alf_queue_count(rpg_page_pool.req_list));
    return 0;
}


int put_req_item(void *tmp)
{
    if (alf_mp_enqueue(rpg_page_pool.req_list, (void **)&tmp, 1) != 1){
	printk("mempool[%s]: Fail to put req item \n",__func__);
	return 1;
    }
    //printk("mempool[%s]:put item count=%u \n", __func__,alf_queue_count(rpg_page_pool.req_list));
    return 0;
}

int get_req_item(void **tmp)
{
    if (alf_queue_empty(rpg_page_pool.req_list)){
	return 1;
    }

    if (alf_mc_dequeue(rpg_page_pool.req_list, (void **)tmp, 1) == 0){
	return 1;
    }
    //printk("mempool[%s]:get item count=%u \n", __func__,alf_queue_count(rpg_page_pool.req_list));
    return 0;
}

int put_sending_index(void *tmp)
{
    if (alf_mp_enqueue(rpg_page_pool.sending_list, (void **)&tmp, 1) != 1){
	printk("mempool[%s]: Fail to put sending index \n",__func__);
	return 1;
    }
    return 0;
}

int get_sending_index(void **tmp)
{
    if (alf_queue_empty(rpg_page_pool.sending_list)){
	return 1;
    }

    if (alf_mc_dequeue(rpg_page_pool.sending_list, (void **)tmp, 1) == 0){
	return 1;
    }
    return 0;
}

//int put_reclaimable_index(struct tree_entry *tmp)
int put_reclaimable_index(void *tmp)
{
    //unsigned long flags;

    //spin_lock_irqsave(&rpg_page_pool.rlock, flags);
    if (alf_mp_enqueue(rpg_page_pool.reclaim_list, (void **)&tmp, 1) != 1){
	printk("mempool[%s]: Fail to put reclaimable index \n",__func__);
        //spin_unlock_irqrestore(&rpg_page_pool.rlock, flags);
	return 1;
    }
    //spin_unlock_irqrestore(&rpg_page_pool.rlock, flags);

   // printk("mempool[%s]:Put Reclaimable item start_index=%zu len=%d reclaimable_count=%u \n",__func__,
   //          tmp->start_index,tmp->len,alf_queue_count(rpg_page_pool.reclaim_list));
    
    return 0;
}

// TODO : failure exception handling
int get_reclaimable_index(void **tmp)
{
    //unsigned long flags;

    //spin_lock_irqsave(&rpg_page_pool.rlock, flags);
    if (alf_queue_empty(rpg_page_pool.reclaim_list)){
        //spin_unlock_irqrestore(&rpg_page_pool.rlock, flags);
	return 1;
    }

    if (alf_mc_dequeue(rpg_page_pool.reclaim_list, (void **)tmp, 1) == 0){
        //spin_unlock_irqrestore(&rpg_page_pool.rlock, flags);
	return 1;
    }
    //spin_unlock_irqrestore(&rpg_page_pool.rlock, flags);

    //printk("mempool[%s]:Get Reclaimable item start_index=%zu len=%d reclaimable_count=%u \n",__func__,
    //         (*tmp)->start_index,(*tmp)->len,alf_queue_count(rpg_page_pool.reclaim_list));
    
    return 0;
}

static void rpg_add_element(rpg_mempool_t *pool, void *element)
{
	unsigned long flags;

        spin_lock_irqsave(&pool->lock, flags);
        if(unlikely(pool->curr_nr >= pool->cap_nr)){
            //panic("rpg_add_element pool->curr_nr >= pool->cap_nr");
	    __free_pages(element,0);
            spin_unlock_irqrestore(&pool->lock, flags);
	    return;
        }
	pool->elements[pool->curr_nr++] = element;
        pool->used_nr--;
        spin_unlock_irqrestore(&pool->lock, flags);
}

static void *rpg_remove_element(rpg_mempool_t *pool)
{
	unsigned long flags;
	void * element;

        spin_lock_irqsave(&pool->lock, flags);
        if (pool->curr_nr > 0) {
           if(unlikely(pool->curr_nr <= 0)){
                panic("rpg_add_element pool->curr_nr <= 0");
           }
           element = pool->elements[--pool->curr_nr];
           pool->used_nr++;
        }else{
 	    element = NULL;
        }
        spin_unlock_irqrestore(&pool->lock, flags);

	return element;
}

// real delete
static void rpg_free_pool(rpg_mempool_t *pool)
{
	while (pool->curr_nr) {
		void *element = rpg_remove_element(pool);
		__free_pages(element,0);
	}
	//kfree(pool->elements);
	vfree(pool->elements);
	//kfree(pool);
	vfree(pool);
}


void * rpg_alloc()
{
    return rpg_mempool_alloc(&rpg_page_pool);
}

void * rpg_mempool_alloc(rpg_mempool_t *pool)
{
	void *element;
	unsigned long flags;
        int err;

        // only alloc from mempool
	element = rpg_remove_element(pool);
	if(!element){
repeat:
		element = rpg_mempool_reclaim(pool);
		if(!element){
	            //printk("mempool[%s]: keep trying to reclaim \n",__func__);
		    goto repeat;
		}
	}

	return element;
}

void rpg_free(void *element)
{
    rpg_mempool_free(element, &rpg_page_pool);
}

void rpg_mempool_free(void *element, rpg_mempool_t *pool)
{
	unsigned long flags;

	if (unlikely(element == NULL))
		return;

	// freed page return to mempool
	clear_highpage(element);
	rpg_add_element(pool, element);
}

void * rpg_mempool_reclaim(rpg_mempool_t *pool)
{
        struct tree_entry *entry;
        void *element=NULL;
	int err;

        err = get_reclaimable_index(&entry);
        if(err){
	    //printk("mempool[%s]: No Reclaimable page. \n",__func__);
	    return NULL;
        }

        if(unlikely(test_flag(entry,UPDATING))){
             //printk("mempool[%s]: UPDATING. skip. index=%zu\n", __func__,entry->index);
	     return NULL;
        }
        clear_flag(entry,RECLAIMABLE);
        if(!entry->page){
             //printk("mempool[%s]: Already Reclaimed. skip. index=%zu\n", __func__,entry->index);
	     return NULL;
        }
        element = entry->page;
        entry->page = NULL;

	if(unlikely(!element)){
	    printk("mempool[%s]: Page is NULL! index=%zu \n",__func__,entry->index);
        }

	return element;
}

//rpg_mempool_t *rpg_mempool_create(rpg_mempool_t *pool, int cap_nr)
int rpg_mempool_create(rpg_mempool_t *pool, long cap_nr)
{
        unsigned long flags;
	size_t freemem_pages;

	printk("mempool[%s]: start mempool create cap_nr=%d \n",__func__,cap_nr);

	/*
	rpg_mempool_t *pool;
	pool = kzalloc(sizeof(*pool), GFP_KERNEL | __GFP_ZERO);
	if (!pool){
		printk("mempool[%s]: Fail to create pool  \n",__func__);
		
		return NULL;
	}
	*/
	//pool->elements = kmalloc(cap_nr * sizeof(void *),GFP_KERNEL);
	pool->elements = vmalloc(cap_nr * sizeof(void *));
	if (!pool->elements) {
		printk("mempool[%s]: Fail to create pool elements  \n",__func__);
		//kfree(pool);
		vfree(pool);
		//return NULL;
		return 1;
	}
	pool->cap_nr = cap_nr;
	pool->threshold = pool->cap_nr - (pool->cap_nr >> 2); // threshold = 75%
	pool->curr_nr = 0;
	pool->used_nr = 0;
	//init_waitqueue_head(&pool->wait);

	// First pre-allocate the guaranteed number of buffers.
	while (pool->curr_nr < pool->cap_nr) {
		void *element;
		element = alloc_page(GFP_NOIO | __GFP_HIGHMEM);
		if (unlikely(!element)) {
			printk("mempool[%s]: Fail to alloc_page  \n",__func__);
			rpg_free_pool(pool);
			//return NULL;
			return 1;
		}
                spin_lock_irqsave(&pool->lock, flags);
	        pool->elements[pool->curr_nr++] = element;
                spin_unlock_irqrestore(&pool->lock, flags);
	}

        freemem_pages = (size_t)info.freeram;
        pool->threshold_page_shrink = freemem_pages * default_mempool_shrink_perc / 100;
        pool->threshold_page_expand = freemem_pages * default_mempool_expand_perc / 100;

        printk("mempool[%s]: Create Done. used=%zu, curr=%zu, threshold=%zu, cap=%zu \n",__func__,pool->used_nr, pool->curr_nr, pool->threshold, pool->cap_nr);
	//return pool;
	return 0;
}

static int mempool_thread_fn(void *data)
{
    int i;
    int err;
    size_t freemem_pages;
    rpg_mempool_t *pool = data;

    while(true)
    {
        si_meminfo(&info);
        freemem_pages = (size_t)info.freeram;

        if(freemem_pages < pool->threshold_page_shrink){
	   pool->state = MEMP_SHRINK;
           printk("mempool: SHRINK freemem_size=%zuGB, freemem_pages=%zu, threshold_page_shrink=%zu, threshold_page_expand=%zu \n",
                          (freemem_pages * (size_t)info.mem_unit) >> 30, freemem_pages, pool->threshold_page_shrink, pool->threshold_page_expand);

	   if(pool->cap_nr <= RESIZING_UNIT_IN_PAGES){
               pool->new_cap_nr = pool->min_pool_pages;
	   }else{
               pool->new_cap_nr = pool->cap_nr - RESIZING_UNIT_IN_PAGES;
	   }
           pool->threshold = pool->new_cap_nr;

           if(pool->new_cap_nr < pool->min_pool_pages){
              printk("mempool: Cap limited by min new_cap_nr=%zu, min_pool_pages=%zu \n",
                     pool->new_cap_nr, pool->min_pool_pages);
              pool->new_cap_nr = pool->min_pool_pages;
              pool->threshold = pool->new_cap_nr;
           }
           err = rpg_mempool_shrink(pool, pool->new_cap_nr);
           if(err){
               printk("mempool: Fail Shrinking (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
                   pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);

               continue;
           }
           pool->state = MEMP_IDLE;
           printk("mempool: Done Shrinking (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
                   pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);

        }else if(freemem_pages > pool->threshold_page_expand){
	   pool->state = MEMP_EXPAND;

       	   if(pool->used_nr >= pool->threshold){
	       //pool->state=RESIZE_START;
               //printk("mempool: Threshold Triggered used=%zu, threshold=%zu, curr=%zu, cap=%zu \n",
                        //pool->used_nr, pool->threshold, pool->curr_nr, pool->cap_nr);
	       if( pool->max_pool_pages == pool->cap_nr){
		    // Already reached max. do reclaim
/*
		    element = rpg_mempool_reclaim(pool);
		    if(element){
			rpg_free(element);
		    }
*/
           	    msleep(2000);
	            continue;
               }else{//able to expand
	           printk("mempool: EXPAND freemem_size=%zuGB, freemem_pages=%zu, threshold_page_shrink=%zu, threshold_page_expand=%zu \n",
                          (freemem_pages * (size_t)info.mem_unit) >> 30, freemem_pages, pool->threshold_page_shrink, pool->threshold_page_expand);

	 	    pool->new_cap_nr = pool->cap_nr + RESIZING_UNIT_IN_PAGES;
                    if(pool->new_cap_nr > pool->max_pool_pages){
                        printk("mempool: Cap limited by max new_cap_nr=%zu, min_pool_pages=%zu \n",
                               pool->new_cap_nr, pool->min_pool_pages);
                        pool->new_cap_nr = pool->max_pool_pages;
                    }
                    err = rpg_mempool_expand(pool, pool->new_cap_nr);
                    if(err){
	                printk("mempool: Fail Expanding (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
                               pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);
	                //pool->state = RESIZE_DONE;
            	        pool->state = MEMP_IDLE;
                        continue;
	             }
                     printk("mempool: Done Expanding (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
                            pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);

               }//able to expand

           }else{//not threshold triggered
                msleep(2000);
	        continue;
           }
          
        }else{//thresh_shrink < freemem < thresh_expand
            pool->state = MEMP_IDLE;

            // Not able to expand. So, periodic reclaim in advance. 
	    if(pool->used_nr >= pool->threshold){
   	       // do reclaim
/*
		    element = rpg_mempool_reclaim(pool);
		    if(element){
			rpg_free(element);
		    }
*/
           	msleep(2000);
           }else{//not threshold triggered
                msleep(2000);
           }
        }//thresh_shrink < freemem < thresh_expand

    }//while

    //never reach here
    return 1;
}

// TODO: resize should be fixed later. It needs to be dynamic resizing
//       without interference of pre-allocated mem.
int rpg_mempool_shrink(rpg_mempool_t *pool, long new_cap_nr)
{
	void *element;
	int i;
	unsigned long flags;
        long unitsize = RESIZING_UNIT_IN_PAGES;

	if(new_cap_nr <= 0)
	    panic("new_cap_nr <= 0");

        printk("mempool: Start Shrinking (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
                   pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);

        // TODO : fix shrink
	spin_lock_irqsave(&pool->lock, flags);
        if (new_cap_nr < pool->cap_nr) {
	       printk("mempool[%s]: starts to shrink \n",__func__);
               while (new_cap_nr < pool->curr_nr) {
                       spin_unlock_irqrestore(&pool->lock, flags);
                       element = rpg_remove_element(pool);
                       __free_pages(element,0);
                       spin_lock_irqsave(&pool->lock, flags);
               }
               pool->cap_nr = new_cap_nr;
               pool->threshold = pool->cap_nr; // in case of shrink, not allow to trigger resize.
               spin_unlock_irqrestore(&pool->lock, flags);
	       return 0;
        }
        spin_unlock_irqrestore(&pool->lock, flags);

        if(pool->min_pool_pages == pool->cap_nr){
	    // Already reached min. do reclaim
            while (unitsize > 0) {
	        element = rpg_mempool_reclaim(pool);
	            if(element){
		        rpg_free(element);
                    }
		unitsize--;
	    }
	}
}

// TODO: resize should be fixed later. It needs to be dynamic resizing
//       without interference of pre-allocated mem.
int rpg_mempool_expand(rpg_mempool_t *pool, long new_cap_nr)
{
	void *element;
	void **new_elements;
	void **tmp_elements;
	int i;
	unsigned long flags;

	if(new_cap_nr <= 0)
	    panic("new_cap_nr <= 0");

	printk("mempool: Start Expanding (used_nr=%zu, cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
                pool->used_nr, pool->cap_nr, pool->curr_nr, new_cap_nr);

       /* Grow the pool */
       //new_elements = kmalloc(new_cap_nr * sizeof(*new_elements),GFP_KERNEL);
       new_elements = vmalloc(new_cap_nr * sizeof(*new_elements));
       if (!new_elements){
	       printk("mempool[%s]: faile to alloc new_elements \n",__func__);
               return 1;
       }

       spin_lock_irqsave(&pool->lock, flags);
       memcpy(new_elements, pool->elements,
                       pool->curr_nr * sizeof(*new_elements));
       tmp_elements = pool->elements;
       pool->elements = new_elements;
       pool->cap_nr = new_cap_nr;
       pool->threshold = pool->cap_nr - (pool->cap_nr >> 2);
       spin_unlock_irqrestore(&pool->lock, flags);
       //kfree(tmp_elements);
       vfree(tmp_elements);

        // grow mempool
        for(i=0;i<RESIZING_UNIT_IN_PAGES;i++){
	    element = alloc_page(GFP_NOIO | __GFP_HIGHMEM);
	    if (!element){
		printk("mempool[%s]: alloc_page fail \n", __func__);
		continue;
            }
            spin_lock_irqsave(&pool->lock, flags);
	    pool->elements[pool->curr_nr++] = element;
            spin_unlock_irqrestore(&pool->lock, flags);
	}

	return 0;
}

// TODO : dest alf_queue
void rpg_mempool_destroy(rpg_mempool_t *pool)
{
	// TODO : why need ?
	// Check for outstanding elements */
	BUG_ON(pool->curr_nr != pool->cap_nr);

	rpg_free_pool(pool);
	// copy rpg_free_pool below instead of calling
	while (pool->curr_nr) {
		void *element = rpg_remove_element(pool);
		__free_pages(element,0);
	}
	//kfree(pool->elements);
	vfree(pool->elements);
	//kfree(pool);
	vfree(pool);
}

static void set_mempool_size(size_t request_min, size_t request_max)
{
  si_meminfo(&info);
  size_t freemem_pages = (size_t)info.freeram;

  // set min_pool_pages
  if (!request_min) {
      rpg_page_pool.min_pool_pages = default_mempool_min * (freemem_pages / 100);
      printk("mempool: Using default for Min Mempool Size (%d%% of RAM) \n", default_mempool_min);
  }else{
    rpg_page_pool.min_pool_pages = request_min;
  }
  printk("mempool: FreeMem Size: %zu GB \nmempool: Min Mempool Pages: %zu \nmempool: Min Mempool size: %zu GB \n",
         (freemem_pages * (size_t)info.mem_unit) >> 30, rpg_page_pool.min_pool_pages, (rpg_page_pool.min_pool_pages * (size_t)info.mem_unit) >> 30
  );

  // set max_pool_pages
  if (!request_max) {
      rpg_page_pool.max_pool_pages = default_mempool_max * (freemem_pages / 100);
      printk("mempool: Using default for Max Mempool Size (%d%% of RAM) \n", default_mempool_max);
  }else{
    rpg_page_pool.max_pool_pages = request_max;
  }
  printk("mempool: FreeMem Size: %zu GB \nmempool: Max Mempool Pages: %zu \nmempool: Max Mempool size: %zu GB \n",
         (freemem_pages * (size_t)info.mem_unit) >> 30, rpg_page_pool.max_pool_pages, (rpg_page_pool.max_pool_pages * (size_t)info.mem_unit) >> 30
  );
}

int is_mempool_init_done()
{
    return rpg_page_pool.init_done;
}

int mempool_init()
{
    size_t index;
    struct local_page_list *tmp;
    int err;
    char thread_name[10]="mempool_t";

    rpg_page_pool.init_done = 0;

    spin_lock_init(&rpg_page_pool.lock);
    spin_lock_init(&rpg_page_pool.slock);
    spin_lock_init(&rpg_page_pool.rlock);
    spin_lock_init(&rpg_page_pool.flock);

    // TODO : fix auto size set case
    //set_mempool_size(0,0);
    //set_mempool_size(786432,6553600); //3GB 25GB
    //set_mempool_size(10485760,5242880); //40GB 20GB in pages
    //set_mempool_size(6553600,5242880); //25GB 20GB in pages
    //set_mempool_size(5242880,5242880); //20GB 20GB in pages
    //set_mempool_size(3932160,5242880); //15GB 20GB in pages
    //set_mempool_size(3145728,5242880); //12GB 20GB in pages
    //set_mempool_size(3145728,5242880); //12GB 20GB in pages
    //set_mempool_size(3145728,3145728); //12GB 12GB in pages
    //set_mempool_size(2097152,3145728); //8GB 12GB in pages
    //set_mempool_size(1572864,3145728); //6GB 12GB in pages
    //set_mempool_size(1179648,3145728); //4.5GB 12GB in pages
    //set_mempool_size(1048576,3145728); //4GB 12GB in pages
    //set_mempool_size(917504,3145728); //3.5GB 12GB in pages
    //set_mempool_size(786432,3145728); //3GB 12GB in pages
    //set_mempool_size(655360,3145728); //2.5GB 12GB in pages
    //set_mempool_size(524288,3145728); //2GB 12GB in pages
    //set_mempool_size(393216,3145728); //1.5GB 12GB in pages
    //set_mempool_size(262144,3145728); //1GB 12GB in pages
    //set_mempool_size(131072,3145728); //0.5GB 12GB in pages
    //set_mempool_size(131072,6553600); //0.5GB 25GB in pages
    set_mempool_size(16384,3145728); //64MB 12GB in pages
    //set_mempool_size(8192,3145728); //32MB 12GB in pages

    printk("mempool[%s]: mempool init start. mempool min pages:%zu mempool max pages=%zu submit_queues:%d \n",__func__,rpg_page_pool.min_pool_pages, rpg_page_pool.max_pool_pages,submit_queues);

    err = rpg_mempool_create(&rpg_page_pool, rpg_page_pool.min_pool_pages);
    if (err){
       printk("mempool[%s]: Fail to create mempool  \n",__func__);
        
       goto fail;
     }

    // TODO : alf_queue dynamically grow shrink
    rpg_page_pool.sending_list = alf_queue_alloc(rpg_page_pool.min_pool_pages, GFP_KERNEL);
    if (IS_ERR_OR_NULL(rpg_page_pool.sending_list)){
       printk("mempool[%s]: Fail to alloc sending_list \n",__func__);
        
       goto fail;
    }
    rpg_page_pool.reclaim_list = alf_queue_alloc(rpg_page_pool.min_pool_pages, GFP_KERNEL);
    if (IS_ERR_OR_NULL(rpg_page_pool.reclaim_list)){
       printk("mempool[%s]: Fail to alloc reclaim_list \n", __func__);
        
       goto fail;
    }  
/*
    rpg_page_pool.freepool_list = alf_queue_alloc(submit_queues*RDMABOX_QUEUE_DEPTH, GFP_KERNEL);
    if (IS_ERR_OR_NULL(rpg_page_pool.freepool_list)){
       printk("mempool[%s]: Fail to alloc freepool_list \n",__func__);
        
       goto fail;
    }  

    rpg_page_pool.msg_freepool_list = alf_queue_alloc(submit_queues*RDMABOX_QUEUE_DEPTH, GFP_KERNEL);
    if (IS_ERR_OR_NULL(rpg_page_pool.msg_freepool_list)){
       printk("mempool[%s]: Fail to alloc freepool_list \n",__func__);
        
       goto fail;
    }  
*/
    // write req list
    rpg_page_pool.req_list = alf_queue_alloc(submit_queues*RDMABOX_QUEUE_DEPTH, GFP_KERNEL);
    if (IS_ERR_OR_NULL(rpg_page_pool.req_list)){
       printk("mempool[%s]: Fail to alloc req_list \n",__func__);
        
       goto fail;
    }  
    // read req list
    rpg_page_pool.read_req_list = alf_queue_alloc(submit_queues*RDMABOX_QUEUE_DEPTH, GFP_KERNEL);
    if (IS_ERR_OR_NULL(rpg_page_pool.req_list)){
       printk("mempool[%s]: Fail to alloc req_list \n",__func__);
        
       goto fail;
    }  

    // start periodic reclaim thread
    printk("mempool[%s]: create thread (%s) \n",__func__,thread_name);
    rpg_page_pool.mempool_thread = kthread_create(mempool_thread_fn, &rpg_page_pool, thread_name);
    wake_up_process(rpg_page_pool.mempool_thread);

    rpg_page_pool.init_done = 1;
    printk("mempool[%s]: mempool init done. \n",__func__);

    return 0;
fail:
    printk("mempool[%s]: mempool init failed. \n",__func__);
    
    return 1;
}
