/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
 *
 */
#ifndef RPG_MEMPOOL_H
#define RPG_MEMPOOL_H

#include <semaphore.h>

#include "urdmabox.h"

// 1GB step increasing for mempool_resize
#define RESIZING_UNIT_IN_PAGES 262144 //1GB


/* Default mempool min size: 5% of Free RAM */
static size_t default_mempool_min = 5;
/* Default mempool max size: 40% of Free  RAM */
static size_t default_mempool_max = 40;

static size_t default_mempool_shrink_perc = 15;
static size_t default_mempool_expand_perc = 30;

// num of free local_page_list
#define FREE_LOCAL_PAGE_LIST_SIZE 1048576

enum mempool_state {
        MEMP_IDLE = 1,
        MEMP_SHRINK,
        MEMP_EXPAND
};
/*
enum freemem_state {
        FREEMEM_STAY = 1,
        FREEMEM_SHRINK,
        FREEMEM_EXPAND
};
*/
struct local_page_list{
        struct atomic_t *ref_count;
        size_t start_index;
	unsigned long start_offset;
        size_t len;
	struct tree_entry *batch_list[RDMA_WR_BUF_LEN];
	unsigned long size_list[RDMA_WR_BUF_LEN]
};

typedef struct rpg_mempool_s {
	pthread_spinlock_t lock;
	pthread_spinlock_t slock;
	pthread_spinlock_t rlock;

	long cap_nr;		/* capacity. nr of elements at *elements */
	long new_cap_nr;		/* new capacity. nr of elements at *elements */
	long curr_nr;		/* curr nr of elements at *elements */
	long used_nr;		/* used nr of elements at *elements */
	long threshold;

	void **elements;

	int init_done;

        long min_pool_pages;
        long max_pool_pages;
	long threshold_page_shrink;
        long threshold_page_expand;

	lfqueue_t *sending_list;
    	lfqueue_t *reclaim_list;
    	lfqueue_t *req_list;
    	lfqueue_t *read_req_list;
    	lfqueue_t *free_local_page_list;

        enum mempool_state state;
	struct task_struct *mempool_thread;
} rpg_mempool_t;

static rpg_mempool_t rpg_page_pool;

extern int mempool_init();
extern int rpg_mempool_create(rpg_mempool_t *pool, long cap_nr);
extern int rpg_mempool_resize(rpg_mempool_t *pool, long new_cap_nr);
extern void rpg_mempool_destroy(rpg_mempool_t *pool);

extern void * rpg_mempool_alloc(rpg_mempool_t *pool);
extern void rpg_mempool_free(void *element, rpg_mempool_t *pool);
extern void * rpg_alloc();
extern void rpg_free(void *element);

extern void * rpg_mempool_reclaim(rpg_mempool_t *pool);

extern struct local_page_list* get_free_item();
extern int put_free_item(struct local_page_list *tmp);
extern struct rdmabox_msg* get_free_msg();
extern int put_free_msg(struct rdmabox_msg *tmp);
//int put_item(lfqueue_t * list, void *item);
//int get_item(lfqueue_t * list, void **item);
extern int get_sending_list_size();
extern int put_sending_index(void *tmp);
extern int get_sending_index(void **tmp);
extern int put_reclaimable_index(void *tmp);
extern int get_reclaimable_index(void **tmp);
extern int put_req_item(void *tmp);
extern int get_req_item(void **tmp);
extern int put_read_req_item(void *tmp);
extern int get_read_req_item(void **tmp);

extern int is_mempool_init_done();


#endif /* RPG_MEMPOOL_H */
