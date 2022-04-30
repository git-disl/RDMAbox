/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
 *
*/
#include <syslog.h>

#include "debug.h"
#include "rpg_mempool.h"
#include "radixtree.h"

long get_free_mem(void)
{
    char result[60];
    FILE *fd = fopen("/proc/meminfo", "r");
    int i;
    long res = 0;
    fgets(result, 60, fd);
    memset(result, 0x00, 60);
    fgets(result, 60, fd);
    for (i=0;i<60;i++){
	if (result[i] >= 48 && result[i] <= 57){
	    res *= 10;
	    res += (int)(result[i] - 48);
	}
    }
    fclose(fd);
    return res;
}

int put_free_item(struct local_page_list *tmp)
{  
    /*
    free(tmp);
    return 0;
    */
    if (lfqueue_enq(rpg_page_pool.free_local_page_list, tmp) < 0){
	syslog(LOG_NOTICE,"mempool[%s]: Fail to put req item \n",__func__);
	return 1;
    }
    return 0;

}

struct local_page_list* get_free_item(void **tmp)
{
    /*
    struct local_page_list *tmp;
    tmp = (struct local_page_list *)malloc(sizeof(struct local_page_list));
    if(!tmp){
	DEBUG_LOG(LOG_NOTICE,"mempool[%s]: Fail to allocate free item \n", __func__);
	return NULL;
    }
    tmp->ref_count = (struct atomic_t *)malloc(sizeof(struct atomic_t));
    atomic_init(tmp->ref_count);
    return tmp;
    */

    if (lfqueue_size(rpg_page_pool.free_local_page_list) == 0){
	return 1;
    }

    *tmp = (struct urdmabox_req*)lfqueue_deq_must(rpg_page_pool.free_local_page_list);
    if(!tmp){
	return 1;
    }
    return 0;
}

/*
//no use ?
int put_item(lfqueue_t * list, void *item)
{
    if (lfqueue_enq(list, item) < 0){
	DEBUG_LOG(LOG_NOTICE,"mempool[%s]: Fail to put sending index \n", __func__);
	return 1;
    }

    return 0;
}

int get_item(lfqueue_t * list, void **item)
{
    if (lfqueue_size(list) == 0){
	return 1;
    }

    *item = (struct urdmabox_req*)lfqueue_deq_must(list);
    if(!item){
	return 1;
    }
    return 0;

}
*/
int put_read_req_item(void *tmp)
{
    if (lfqueue_enq(rpg_page_pool.read_req_list, tmp) < 0){
	syslog(LOG_NOTICE,"mempool[%s]: Fail to put req item \n",__func__);
	return 1;
    }
    return 0;
}

int get_read_req_item(void **tmp)
{
    if (lfqueue_size(rpg_page_pool.read_req_list) == 0){
	return 1;
    }

    *tmp = (struct urdmabox_req*)lfqueue_deq_must(rpg_page_pool.read_req_list);
    if(!tmp){
	return 1;
    }
    return 0;
}


int put_req_item(void *tmp)
{
    if (lfqueue_enq(rpg_page_pool.req_list, tmp) < 0){
	syslog(LOG_NOTICE,"mempool[%s]: Fail to put req item \n",__func__);
	return 1;
    }
    return 0;
}

int get_req_item(void **tmp)
{
    if (lfqueue_size(rpg_page_pool.req_list) == 0){
	return 1;
    }

    *tmp = (struct urdmabox_req*)lfqueue_deq_must(rpg_page_pool.req_list);
    if(!tmp){
	return 1;
    }
    return 0;
}

int get_sending_list_size()
{
    return lfqueue_size(rpg_page_pool.sending_list);
}

int put_sending_index(void *tmp)
{
    //pthread_spin_lock(&rpg_page_pool.slock);
    if (lfqueue_enq(rpg_page_pool.sending_list, tmp) < 0){
        //pthread_spin_unlock(&rpg_page_pool.slock);
	syslog(LOG_NOTICE,"mempool[%s]: Fail to put req item \n",__func__);
	return 1;
    }
    //pthread_spin_unlock(&rpg_page_pool.slock);
    //DEBUG_LOG(LOG_NOTICE,"mempool[%s]: put sending item %d \n",__func__,lfqueue_size(rpg_page_pool.sending_list));

    return 0;
}

int get_sending_index(void **tmp)
{
    //pthread_spin_lock(&rpg_page_pool.slock);
    if (lfqueue_size(rpg_page_pool.sending_list) == 0){
        //pthread_spin_unlock(&rpg_page_pool.slock);
         //DEBUG_LOG(LOG_NOTICE,"mempool[%s]: no sending item %d \n",__func__,lfqueue_size(rpg_page_pool.sending_list));
	return 1;
    }

    *tmp = (struct urdmabox_req*)lfqueue_deq_must(rpg_page_pool.sending_list);
    if(!tmp){
        //pthread_spin_unlock(&rpg_page_pool.slock);
        syslog(LOG_NOTICE,"mempool[%s]: fail to dequeue sending item \n",__func__);
	return 1;
    }
    //pthread_spin_unlock(&rpg_page_pool.slock);

    //DEBUG_LOG(LOG_NOTICE,"mempool[%s]: get sending item %d \n",__func__,lfqueue_size(rpg_page_pool.sending_list));

    return 0;
}

int put_reclaimable_index(void *tmp)
{
    //pthread_spin_lock(&rpg_page_pool.rlock);
    if (lfqueue_enq(rpg_page_pool.reclaim_list, tmp) < 0){
        //pthread_spin_unlock(&rpg_page_pool.rlock);
	syslog(LOG_NOTICE,"mempool[%s]: Fail to put reclaimable index \n",__func__);
	return 1;
    }

    return 0;
}

// TODO : failure exception handling
int get_reclaimable_index(void **tmp)
{
    if (lfqueue_size(rpg_page_pool.reclaim_list) == 0){
	return 1;
    }

    //pthread_spin_lock(&rpg_page_pool.rlock);
    *tmp = (struct urdmabox_req*)lfqueue_deq_must(rpg_page_pool.reclaim_list);
    if(!tmp){
        //pthread_spin_unlock(&rpg_page_pool.rlock);
	return 1;
    }
    //pthread_spin_unlock(&rpg_page_pool.rlock);

    return 0;
}

static void rpg_add_element(rpg_mempool_t *pool, void *element)
{
    unsigned long flags;

    pthread_spin_lock(&pool->lock);
    if(pool->curr_nr >= pool->cap_nr){
	free(element);
        pthread_spin_unlock(&pool->lock);
	return;
    }
    pool->elements[pool->curr_nr++] = element;
    pool->used_nr--;
    pthread_spin_unlock(&pool->lock);
}

static void *rpg_remove_element(rpg_mempool_t *pool)
{
    unsigned long flags;
    void * element;

    pthread_spin_lock(&pool->lock);
    if (pool->curr_nr > 0) {
	if(pool->curr_nr <= 0){
	    printf("pool->curr_nr <= 0\n");
	    element = NULL;
	}
	element = pool->elements[--pool->curr_nr];
	pool->used_nr++;
    }else{
	element = NULL;
    }
    pthread_spin_unlock(&pool->lock);

    return element;
}

static void rpg_free_pool(rpg_mempool_t *pool)
{
    while (pool->curr_nr) {
	void *element = rpg_remove_element(pool);
	free(element);
    }
    free(pool->elements);
    free(pool);
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
	    //DEBUG_LOG(LOG_NOTICE,"mempool[%s]: keep trying to reclaim \n",__func__);
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

    if (element == NULL)
	return;

    // freed page return to mempool
    rpg_add_element(pool, element);
}

void * rpg_mempool_reclaim(rpg_mempool_t *pool)
{
    struct tree_entry *entry;
    void *element=NULL;
    int err;

    err = get_reclaimable_index(&entry);
    if(err){
	//DEBUG_LOG(LOG_NOTICE,"mempool[%s]: No Reclaimable page. \n",__func__);
	return NULL;
    }

    if(test_flag(entry,UPDATING)){
	//DEBUG_LOG(LOG_NOTICE,"mempool[%s]: UPDATING. skip. index=%zu\n", __func__,entry->index);
	return NULL;
    }
    clear_flag(entry,RECLAIMABLE);
    if(!entry->page){
	//DEBUG_LOG(LOG_NOTICE,"mempool[%s]: Already Reclaimed. skip. index=%zu\n", __func__,entry->index);
	return NULL;
    }
    element = entry->page;
    entry->page = NULL;

    if(!element){
	//DEBUG_LOG(LOG_NOTICE,"mempool[%s]: Page is NULL! index=%zu \n",__func__,entry->index);
    }

    return element;
}

int rpg_mempool_create(rpg_mempool_t *pool, long cap_nr)
{
    unsigned long flags;
    size_t freemem_pages;

    DEBUG_LOG(LOG_NOTICE,"mempool[%s]: start mempool create cap_nr=%d \n",__func__,cap_nr);

    pool->elements = malloc(cap_nr * sizeof(void *));
    if (!pool->elements) {
	DEBUG_LOG(LOG_NOTICE,"mempool[%s]: Fail to create pool elements  \n",__func__);
	free(pool);
	return 1;
    }
    pool->cap_nr = cap_nr;
    pool->threshold = pool->cap_nr - (pool->cap_nr >> 2); // threshold = 75%
    pool->curr_nr = 0;
    pool->used_nr = 0;

    // First pre-allocate the guaranteed number of buffers.
    while (pool->curr_nr < pool->cap_nr) {
	void *element;
	element = malloc(LOCAL_BLOCK_SIZE);
	if (!element) {
	    DEBUG_LOG(LOG_NOTICE,"mempool[%s]: Fail to alloc_page  \n",__func__);
	    rpg_free_pool(pool);
	    return 1;
	}
        pthread_spin_lock(&pool->lock);
	pool->elements[pool->curr_nr++] = element;
        pthread_spin_unlock(&pool->lock);
    }

    freemem_pages = get_free_mem() / (LOCAL_BLOCK_SIZE/1024);
    pool->threshold_page_shrink = freemem_pages * default_mempool_shrink_perc / 100;
    pool->threshold_page_expand = freemem_pages * default_mempool_expand_perc / 100;

    DEBUG_LOG(LOG_NOTICE,"mempool[%s]: Create Done. used=%zu, curr=%zu, threshold=%zu, cap=%zu \n",__func__,pool->used_nr, pool->curr_nr, pool->threshold, pool->cap_nr);
    return 0;
}

/*
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
   DEBUG_LOG(LOG_NOTICE,"mempool: SHRINK freemem_size=%zuGB, freemem_pages=%zu, threshold_page_shrink=%zu, threshold_page_expand=%zu \n",
   (freemem_pages * (size_t)info.mem_unit) >> 30, freemem_pages, pool->threshold_page_shrink, pool->threshold_page_expand);

   if(pool->cap_nr <= RESIZING_UNIT_IN_PAGES){
   pool->new_cap_nr = pool->min_pool_pages;
   }else{
   pool->new_cap_nr = pool->cap_nr - RESIZING_UNIT_IN_PAGES;
   }
   pool->threshold = pool->new_cap_nr;

   if(pool->new_cap_nr < pool->min_pool_pages){
   DEBUG_LOG(LOG_NOTICE,"mempool: Cap limited by min new_cap_nr=%zu, min_pool_pages=%zu \n",
   pool->new_cap_nr, pool->min_pool_pages);
   pool->new_cap_nr = pool->min_pool_pages;
   pool->threshold = pool->new_cap_nr;
   }
   err = rpg_mempool_shrink(pool, pool->new_cap_nr);
   if(err){
   DEBUG_LOG(LOG_NOTICE,"mempool: Fail Shrinking (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
   pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);

   continue;
   }
   pool->state = MEMP_IDLE;
   DEBUG_LOG(LOG_NOTICE,"mempool: Done Shrinking (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
   pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);

   }else if(freemem_pages > pool->threshold_page_expand){
   pool->state = MEMP_EXPAND;

   if(pool->used_nr >= pool->threshold){
//pool->state=RESIZE_START;
//DEBUG_LOG(LOG_NOTICE,"mempool: Threshold Triggered used=%zu, threshold=%zu, curr=%zu, cap=%zu \n",
//pool->used_nr, pool->threshold, pool->curr_nr, pool->cap_nr);
if( pool->max_pool_pages == pool->cap_nr){
// Already reached max. do reclaim
msleep(2000);
continue;
}else{//able to expand
DEBUG_LOG(LOG_NOTICE,"mempool: EXPAND freemem_size=%zuGB, freemem_pages=%zu, threshold_page_shrink=%zu, threshold_page_expand=%zu \n",
(freemem_pages * (size_t)info.mem_unit) >> 30, freemem_pages, pool->threshold_page_shrink, pool->threshold_page_expand);

pool->new_cap_nr = pool->cap_nr + RESIZING_UNIT_IN_PAGES;
if(pool->new_cap_nr > pool->max_pool_pages){
DEBUG_LOG(LOG_NOTICE,"mempool: Cap limited by max new_cap_nr=%zu, min_pool_pages=%zu \n",
pool->new_cap_nr, pool->min_pool_pages);
pool->new_cap_nr = pool->max_pool_pages;
}
err = rpg_mempool_expand(pool, pool->new_cap_nr);
if(err){
DEBUG_LOG(LOG_NOTICE,"mempool: Fail Expanding (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);
//pool->state = RESIZE_DONE;
pool->state = MEMP_IDLE;
continue;
}
DEBUG_LOG(LOG_NOTICE,"mempool: Done Expanding (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
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

    DEBUG_LOG(LOG_NOTICE,"mempool: Start Shrinking (used_nr=%zu cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
	    pool->used_nr, pool->cap_nr, pool->curr_nr, pool->new_cap_nr);

    // TODO : fix shrink
    spin_lock_irqsave(&pool->lock, flags);
    if (new_cap_nr < pool->cap_nr) {
	DEBUG_LOG(LOG_NOTICE,"mempool[%s]: starts to shrink \n",__func__);
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

    DEBUG_LOG(LOG_NOTICE,"mempool: Start Expanding (used_nr=%zu, cap_nr=%zu, curr_nr=%zu, new_cap_nr=%zu) \n",
	    pool->used_nr, pool->cap_nr, pool->curr_nr, new_cap_nr);

    //new_elements = kmalloc(new_cap_nr * sizeof(*new_elements),GFP_KERNEL);
    new_elements = vmalloc(new_cap_nr * sizeof(*new_elements));
    if (!new_elements){
	DEBUG_LOG(LOG_NOTICE,"mempool[%s]: faile to alloc new_elements \n",__func__);
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
	    DEBUG_LOG(LOG_NOTICE,"mempool[%s]: alloc_page fail \n", __func__);
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
    // Check for outstanding elements
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
*/

static void set_mempool_size(size_t request_min, size_t request_max)
{
    size_t freemem_pages = get_free_mem() / (LOCAL_BLOCK_SIZE/1024);

    // set min_pool_pages
    if (!request_min) {
	rpg_page_pool.min_pool_pages = default_mempool_min * (freemem_pages / 100);
	DEBUG_LOG(LOG_NOTICE,"mempool: Using default for Min Mempool Size (%d%% of RAM) \n", default_mempool_min);
    }else{
	rpg_page_pool.min_pool_pages = request_min/(LOCAL_BLOCK_SIZE/1024);
    }
    DEBUG_LOG(LOG_NOTICE,"mempool: FreeMem Size: %zuMB mempool: MinMempoolPages: %zu mempool: MinMempoolsize: %zuGB \n",
	    get_free_mem() >> 10, rpg_page_pool.min_pool_pages, request_min >> 20
	    );

    // set max_pool_pages
    if (!request_max) {
	rpg_page_pool.max_pool_pages = default_mempool_max * (freemem_pages / 100);
	DEBUG_LOG(LOG_NOTICE,"mempool: Using default for Max Mempool Size (%d%% of RAM) \n", default_mempool_max);
    }else{
	rpg_page_pool.max_pool_pages = request_max/(LOCAL_BLOCK_SIZE/1024);
    }
    DEBUG_LOG(LOG_NOTICE,"mempool: FreeMem Size: %zuMB mempool: MaxMempoolPages: %zu mempool: MaxMempoolsize: %zuGB \n",
	    get_free_mem() >> 10, rpg_page_pool.max_pool_pages, request_max >> 20
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
    int i;
    char thread_name[10]="mempool_t";

    rpg_page_pool.init_done = 0;

    pthread_spin_init(&rpg_page_pool.lock,0);
    pthread_spin_init(&rpg_page_pool.slock,0);
    pthread_spin_init(&rpg_page_pool.rlock,0);

    set_mempool_size(1048576,12582912); //1GB 12GB in KB
    //set_mempool_size(524288,12582912); //500MB 12GB in KB

    DEBUG_LOG(LOG_NOTICE,"mempool[%s]: mempool init start. mempool min pages:%zu mempool max pages=%zu \n",__func__,rpg_page_pool.min_pool_pages, rpg_page_pool.max_pool_pages);

    err = rpg_mempool_create(&rpg_page_pool, rpg_page_pool.min_pool_pages);
    if (err){
	DEBUG_LOG(LOG_NOTICE,"mempool[%s]: Fail to create mempool  \n",__func__);

	goto fail;
    }

    rpg_page_pool.sending_list = malloc(sizeof(lfqueue_t));
    if (lfqueue_init(rpg_page_pool.sending_list) == -1){
	syslog(LOG_NOTICE,"session_create - fail to malloc wr_req_q \n");
	goto fail;
    }
    rpg_page_pool.reclaim_list = malloc(sizeof(lfqueue_t));
    if (lfqueue_init(rpg_page_pool.reclaim_list) == -1){
	syslog(LOG_NOTICE,"session_create - fail to malloc wr_req_q \n");
	goto fail;
    }
    // write req list
    rpg_page_pool.req_list = malloc(sizeof(lfqueue_t));
    if (lfqueue_init(rpg_page_pool.req_list) == -1){
	syslog(LOG_NOTICE,"session_create - fail to malloc wr_req_q \n");
	goto fail;
    }
    // read req list
    rpg_page_pool.read_req_list = malloc(sizeof(lfqueue_t));
    if (lfqueue_init(rpg_page_pool.read_req_list) == -1){
	syslog(LOG_NOTICE,"session_create - fail to malloc wr_req_q \n");
	goto fail;
    }
    rpg_page_pool.free_local_page_list = malloc(sizeof(lfqueue_t));
    if (lfqueue_init(rpg_page_pool.free_local_page_list) == -1){
	syslog(LOG_NOTICE,"session_create - fail to malloc wr_req_q \n");
	goto fail;
    }
    for(i=0; i<FREE_LOCAL_PAGE_LIST_SIZE; i++)
    {
        struct local_page_list *tmp = malloc(sizeof(struct local_page_list));
        tmp->ref_count = (struct atomic_t *)malloc(sizeof(struct atomic_t));
        atomic_init(tmp->ref_count);
        put_free_item(tmp);
    }

    /*
    // start periodic reclaim thread
    DEBUG_LOG(LOG_NOTICE,"mempool[%s]: create thread (%s) \n",__func__,thread_name);
    rpg_page_pool.mempool_thread = kthread_create(mempool_thread_fn, &rpg_page_pool, thread_name);
    wake_up_process(rpg_page_pool.mempool_thread);
     */

    rpg_page_pool.init_done = 1;
    DEBUG_LOG(LOG_NOTICE,"mempool[%s]: mempool init done. \n",__func__);

    return 0;
fail:
    DEBUG_LOG(LOG_NOTICE,"mempool[%s]: mempool init failed. \n",__func__);

    return 1;
}
