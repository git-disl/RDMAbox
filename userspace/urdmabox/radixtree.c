#include <syslog.h>

#include "debug.h"

#include "radixtree.h"
#include "rpg_mempool.h"

size_t int2key(char *s, size_t maxlen, uint32_t i) 
{
    return snprintf(s,maxlen,"%lu",(unsigned long)i);
}

int test_flag(struct tree_entry *entry,enum entry_flags flag)
{
    return entry->flags & (1<<flag);
}

int set_flag(struct tree_entry *entry,enum entry_flags flag)
{
    entry->flags |= (1<<flag);
}

int clear_flag(struct tree_entry *entry,enum entry_flags flag)
{
    entry->flags &= ~(1<<flag);
}

struct tree_entry *brd_lookup_page(struct brd_device *brd, size_t index)
{
    struct tree_entry *entry;
    unsigned char key[1024];
    uint32_t keylen = int2key((char*)key,sizeof(key),index);

    entry = raxFind(brd->brd_pages,key,keylen);

    return entry; 
}

struct tree_entry *brd_lookup(size_t index)
{
    return brd_lookup_page(&brd,index);
}

struct tree_entry* brd_alloc_entry(struct brd_device *brd, size_t index)
{
    struct tree_entry *entry;
    unsigned char key[1024];
    uint32_t keylen = int2key((char*)key,sizeof(key),index);

    entry = (struct tree_entry *)malloc(sizeof(struct tree_entry)); 
    if(!entry){
	DEBUG_LOG(LOG_NOTICE,"brd[%s]: allocate entry fail \n", __func__);
	return NULL;
    }
    entry->page = rpg_alloc();
    if(!entry->page){
	DEBUG_LOG(LOG_NOTICE,"brd[%s]: allocate rpg_alloc fail \n", __func__);
	free(entry);
	return NULL;
    }
    entry->flags=0;
    entry->ref_count = (struct atomic_t *)malloc(sizeof(struct atomic_t));
    atomic_init(entry->ref_count);
    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: entry alloced %lu %s \n", __func__, index, key);

    return entry;
}

struct tree_entry* brd_alloc(size_t index)
{
    return brd_alloc_entry(&brd, index);
}

struct tree_entry* brd_insert_page(struct brd_device *brd, struct tree_entry *entry, size_t index)
{
    unsigned char key[1024];
    uint32_t keylen = int2key((char*)key,sizeof(key),index);

    pthread_mutex_lock(&brd->brd_lock);
    entry = raxFind(brd->brd_pages,key,keylen);
    pthread_mutex_unlock(&brd->brd_lock);
    if(entry == raxNotFound){
	//DEBUG_LOG(LOG_NOTICE,"brd[%s]: entry not found %lu %s \n", __func__, index, key);
	entry = brd_alloc_entry(brd, index);
	if(!entry){
	    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: brd alloc entry fail \n", __func__);
	    return NULL;
	}
	    atomic_set(entry->ref_count,0);
    }else{
	//DEBUG_LOG(LOG_NOTICE,"brd[%s]: entry exists. It is updating transaction. index=%lu \n", __func__,index);
	if(!entry->page){
	    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: entry exists but page is null. rpg_alloc() here \n", __func__);
	    entry->page = rpg_alloc();
	    if(!entry->page){
		DEBUG_LOG(LOG_NOTICE,"brd[%s]: rpg alloc entry->page fail \n", __func__);
		return NULL;
	    }
	}
	    atomic_inc_return(entry->ref_count);
	    set_flag(entry,UPDATING);
	return entry;
    }

	pthread_mutex_lock(&brd->brd_lock);
    if (!raxInsert(brd->brd_pages,key,keylen,(void*)entry,NULL)) {
	pthread_mutex_unlock(&brd->brd_lock);
	DEBUG_LOG(LOG_NOTICE,"brd[%s]: radix_tree_insert fail. entry exists in radix_tree. \n", __func__);
	return NULL;
    }
    pthread_mutex_unlock(&brd->brd_lock);
    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: radix_tree_insert done index=%lu %s count:%u \n", __func__,index,key,raxSize(brd->brd_pages) );

    return entry;
}

struct tree_entry* brd_insert(struct tree_entry *entry, size_t index)
{
    return brd_insert_page(&brd, entry, index);
}

void brd_free_entry(struct brd_device *brd, size_t index)
{
    struct tree_entry *entry;
    unsigned char key[1024];
    uint32_t keylen = int2key((char*)key,sizeof(key),index);

    pthread_mutex_lock(&brd->brd_lock);
    entry = raxFind(brd->brd_pages,key,keylen);
    raxRemove(brd->brd_pages,index,sizeof(index),NULL);
    pthread_mutex_unlock(&brd->brd_lock);
    if (entry){
	rpg_free(entry->page);
	free(entry);
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
    unsigned char key[1024];
    uint32_t keylen = int2key((char*)key,sizeof(key),index);

    pthread_mutex_lock(&brd->brd_lock);
    entry = raxFind(brd->brd_pages,key,keylen);
    if(!entry){
	DEBUG_LOG(LOG_NOTICE,"brd_remove_page : entry is null\n");
	pthread_mutex_unlock(&brd->brd_lock);
	return NULL;
    }
    /*
       if(entry->ref_count > 0){
       DEBUG_LOG(LOG_NOTICE,"brd[%s]: page is updating. skip this page. index=%zu \n", __func__,index);
       return NULL;
       }
     */
    pthread_mutex_unlock(&brd->brd_lock);
    page = entry->page;
    entry->page = NULL;

    return page;
}

/*
   struct page * brd_remove(size_t index)
   {
   return brd_remove_page(&brd, index);
   }

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
   DEBUG_LOG(LOG_NOTICE,"brd[%s]: page is not stored index=%zu \n", __func__,index);
   entry->page = rpg_alloc();
   if (!entry->page)
   return 1;
   }else{
   DEBUG_LOG(LOG_NOTICE,"brd[%s]: page is already stored. Skip. index=%zu \n", __func__,index);
   return 1;
   }
   }

   spin_lock_irqsave(&brd->brd_lock, flags);
   cmem = kmap_atomic(entry->page);
   memcpy(cmem, buffer + (i*PAGE_SIZE), PAGE_SIZE);
   kunmap_atomic(cmem);
   entry->len = PAGE_SIZE;
   spin_unlock_irqrestore(&brd->brd_lock, flags);

   DEBUG_LOG(LOG_NOTICE,"brd[%s]: proactive read page is done. index=%zu \n", __func__,index);

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
   DEBUG_LOG(LOG_NOTICE,"cpu(%d)rdmabox[%s]: page is not stored index=%zu \n",get_cpu(), __func__,index);
   entry->page = rpg_alloc();
   }else{
   DEBUG_LOG(LOG_NOTICE,"cpu(%d)rdmabox[%s]: page is already stored in Local. Skip. index=%zu \n",get_cpu(), __func__,index);
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

/*
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

} while (nr_entries == FREE_BATCH);
}
 */
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
DEBUG_LOG(LOG_NOTICE,"bio_clone_bioset() -> NULL\n");

return NULL;
}

if ( status = bio_alloc_pages(dst , GFP_KERNEL))
{
DEBUG_LOG(LOG_NOTICE,"bio_alloc_pages() -> %d\n", status);

return NULL;
}
return dst;
}
 */
//int write_to_brd(struct brd_device *brd, struct urdmabox_req *req, struct session *sess)
int write_to_brd(struct brd_device *brd, struct session *sess, const char * buf, unsigned long size, unsigned long offset, int start_index)
{
    int ret;
    size_t i,j;
    struct local_page_list *tmp_item;
    struct tree_entry *entry;
    //size_t start_index;
    //start_index = req->block_index;

    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: Write Local start_index=%u, offset:%lu size:%lu\n", __func__,start_index,req->offset, req->size);
    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: Write Local start_index=%u, offset:%lu size:%lu\n", __func__,start_index,offset, size);
repeat:
    //tmp_item = get_free_item();
    get_free_item(&tmp_item);
    if(!tmp_item){
	DEBUG_LOG(LOG_NOTICE,"brd[%s]: Fail to allocate list item. retry \n", __func__);
	goto repeat;
    }
    tmp_item->start_index = start_index;
    //tmp_item->start_offset = req->offset;
    tmp_item->start_offset = offset;
    tmp_item->len = 1; // num of req

    entry = (struct tree_entry *)brd_insert_page(brd, entry, start_index);
    tmp_item->batch_list[0] = entry;
    //tmp_item->size_list[0] = req->size;
    tmp_item->size_list[0] = size;

    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: store to entry:%p offset:%lu size:%lu\n", __func__,entry->page, req->offset, req->size);
    //memcpy(entry->page + req->offset, req->buf, req->size);
    memcpy(entry->page + offset, buf, size);

    // put index sending list here
    // later, it will be moved to recliamable list by remote sender thread
send_again:
    ret = put_sending_index(tmp_item);
    if(ret){
	DEBUG_LOG(LOG_NOTICE,"brd[%s]: Fail to put sending index. retry \n", __func__);
	goto send_again;
    }
/*
    if(get_sending_list_size() >= RDMA_WR_BUF_LEN)
    {
        DEBUG_LOG(LOG_NOTICE,"brd[%s]: notify sending list sending_list_size:%d buffer_len:%d\n", __func__,get_sending_list_size(),RDMA_WR_BUF_LEN);
        notify_sending_list_full();
    }
*/
    // for now, we use remote_sender_full_fn thread
    //remote_sender_fn(sess);
    //remote_sender_full_fn(sess);

    return 0;
}

//int radix_write(struct urdmabox_req *req)
int radix_write(const char * buf, unsigned long size, unsigned long offset, int start_index)
{
    return write_to_brd(&brd, &session, buf, size, offset,start_index);
}

// TODO : batch proactive read
//int read_from_brd(struct brd_device *brd, struct urdmabox_req *req, struct session *sess)
int read_from_brd(struct brd_device *brd, struct session *sess, const char * buf, unsigned long size, unsigned long offset, int start_index)
{
    int i, ret;
    size_t index;
    struct tree_entry *entry;
    struct local_page_list *tmp_item;
    unsigned char key[1024];
    uint32_t keylen;

    //start_index = req->block_index;
    keylen = int2key((char*)key,sizeof(key),start_index);
    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: Read Local start_index=%u \n", __func__,start_index);

    entry = raxFind(brd->brd_pages,key,keylen);
    if(entry != raxNotFound && entry->page){
        //memcpy(req->buf, entry->page + req->offset, req->size);
        memcpy(buf, entry->page + offset, size);

        //DEBUG_LOG(LOG_NOTICE,"brd[%s]: Local hit ! start_index=%u \n", __func__,start_index);
        goto out;
    }

    //DEBUG_LOG(LOG_NOTICE,"brd[%s]: Local miss start_index=%u \n", __func__,start_index);
    return 1;

out:
    return 0;
}

//int radix_read(struct urdmabox_req *req)
int radix_read(const char * buf, unsigned long size, unsigned long offset, int start_index)
{
    return read_from_brd(&brd, &session, buf, size, offset, start_index);
}

int brd_init()
{
    DEBUG_LOG(LOG_NOTICE,"brd[%s]: brd init start \n", __func__);

    brd.init_done = 0;

    pthread_mutex_init(&brd.brd_lock,0);
    brd.brd_pages = raxNew();

    brd.init_done = 1;

    DEBUG_LOG(LOG_NOTICE,"brd[%s]: brd init done \n", __func__);
    return 0;

    return -ENOMEM;
}
/*
   void brd_destroy(struct brd_device *brd)
   {
   brd_free_pages(brd);
   kfree(brd);
   }
 */
