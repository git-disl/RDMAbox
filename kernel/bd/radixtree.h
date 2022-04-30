
#ifndef RADIXTREE_H
#define RADIXTREE_H

#include <linux/init.h>
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/major.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/highmem.h>
#include <linux/mutex.h>
#include <linux/radix-tree.h>
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <asm/uaccess.h>

#include "rdmabox.h"

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
	struct page *page;
	u16 len;
	u8 flags;
	atomic_t ref_count;
};

struct brd_device {
	struct mutex init_lock;
        spinlock_t brd_lock; // used in insert & free
	//spinlock_t brd_ref_count_lock;
        struct radix_tree_root brd_pages;
	int init_done;
	// TODO : decide to use lmempool_size or not
};

static struct brd_device brd;
/*
extern void ref_count_set(int *v, int value);
extern void ref_count_inc(int *v);
extern void ref_count_dec(int *v);
*/
extern int test_flag(struct tree_entry *entry, enum entry_flags flag);
extern int set_flag(struct tree_entry *entry, enum entry_flags flag);
extern int clear_flag(struct tree_entry *entry, enum entry_flags flag);

//extern int radix_read(struct request *req, struct RDMABOX_queue *xq);
int radix_read(struct RDMABOX_session *RDMABOX_sess, struct request *req);
//extern int radix_write(struct request *req, struct RDMABOX_queue *xq);
int radix_write(struct RDMABOX_session *RDMABOX_sess, struct request *req);

extern struct tree_entry *brd_insert(struct tree_entry *entry, size_t index);
extern struct tree_entry *brd_lookup(size_t index);
//extern int brd_update(char *buffer, struct local_page_list *tmp);
//extern struct tree_entry * brd_start_update(struct tree_entry *tmp);
//extern void brd_update_done();

extern struct tree_entry* brd_alloc(size_t index);
extern void brd_free(size_t index);
extern struct page * brd_remove(size_t index);

extern int brd_init(u64 size);
extern void brd_destroy(struct brd_device *brd);
struct bio * create_bio_copy(struct bio *src);

#endif /* RADIXTREE_H */
