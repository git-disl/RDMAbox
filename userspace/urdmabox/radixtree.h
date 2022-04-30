/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
 *
*/
#ifndef RADIXTREE_H
#define RADIXTREE_H

#include "urdmabox.h"
#include "rax.h"

#define SECTOR_SIZE             512
#define SECTOR_SHIFT            ilog2(SECTOR_SIZE)
#define SECTORS_PER_PAGE_SHIFT  (PAGE_SHIFT - SECTOR_SHIFT)
#define SECTORS_PER_PAGE        (1 << SECTORS_PER_PAGE_SHIFT)

/*
 * Each block ramdisk device has a radix_tree brd_pages of pages that stores
 * the pages containing the block device's contents. A brd page's ->index is
 * its offset in PAGE_SIZE units. This is similar to, but in no way connected
 * with, the kernel's pagecache or buffer cache (which sit above our block
 * device).
 */

/* Flags for zram pages (table[page_no].flags) */
enum entry_flags {
        UPDATING,
        RECLAIMABLE,
        RECLAIMED,
};

struct tree_entry {
	size_t index;
	char *page;
	uint8_t flags;
	struct atomic_t *ref_count;
};

struct brd_device {
	pthread_spinlock_t init_lock;
        pthread_mutex_t brd_lock; // used in insert & free
        rax *brd_pages;
	int init_done;
};

static struct brd_device brd;

extern int test_flag(struct tree_entry *entry, enum entry_flags flag);
extern int set_flag(struct tree_entry *entry, enum entry_flags flag);
extern int clear_flag(struct tree_entry *entry, enum entry_flags flag);

//int radix_read(struct urdmabox_req *req);
//int radix_write(struct urdmabox_req *req);

extern int radix_read(const char * buf, unsigned long size, unsigned long offset, int start_index);
extern int radix_write(const char * buf, unsigned long size, unsigned long offset, int start_index);


extern struct tree_entry *brd_insert(struct tree_entry *entry, size_t index);
extern struct tree_entry *brd_lookup(size_t index);

extern struct tree_entry* brd_alloc(size_t index);
//extern void brd_free(size_t index);
//extern struct page * brd_remove(size_t index);

extern int brd_init();
//extern void brd_destroy(struct brd_device *brd);

#endif /* RADIXTREE_H */
