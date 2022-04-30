/*********************************************************
 * 
 *    	Program implementing an in-memory filesystem 
 *			(ie, RAMDISK) using FUSE.  
 *
 *********************************************************/

/*................. Include Files .......................*/ 

#define FUSE_USE_VERSION 34
//#define FUSE_USE_VERSION 29

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <semaphore.h>

#include <sys/syscall.h>

#include "debug.h"

#define NAME_MAX		4096
#define PATH_MAX 	  4096
#define MSIZE 			 2
#define  ROOT			 0

#define BLOCK_SIZE 1048576 //1MB
//#define BLOCK_SIZE 131072 // 128KB

#define USED 1
#define UNUSED 0

#define USE_RDMA

// either writeback or batch in write
//#define USE_RDMABOX_WRITEBACK
#define USE_BATCH_WRITE

#define USE_BATCH_READ

/*............. Global Declarations .................*/

#define U_ATIME (1 << 0)
#define U_CTIME (1 << 1)
#define U_MTIME (1 << 2)

struct block
{
    int inuse;
#ifndef USE_RDMA
    char data[BLOCK_SIZE];
#endif
    int next_block;
};

/* File system Information */
struct fsdata {
    unsigned long free_bytes;
    unsigned long used_bytes;
    unsigned long total_size;
    unsigned long max_no_of_files;
    unsigned long avail_no_of_files;
};

/* File information */
struct metadata {
    unsigned long inode;  
    unsigned long size;
    char *data;
    mode_t mode;
    short inuse;
    time_t accesstime;
    time_t modifiedtime;			  
    int start_block;
    int pos_new_block;
    int **blk_list;
    uid_t uid;
    gid_t gid;
    //struct stat vstat;
};

/* File information maintained by a directory */
struct list {
    char fname [NAME_MAX];
    unsigned long inode;
    struct list *next;
};

/* Directory information */
struct directory {
    char name [PATH_MAX];
    unsigned long inode;
    struct list *ptr;
    struct list *lptr;
    struct directory *next;
};

struct urdmabox_req {
    int block_index;
    const char * buf;
    unsigned long size;
    unsigned long offset;
    void * private_data;
    sem_t *sem;
    int *ref_count;
    void * req;
};

typedef struct freepool_s {
    pthread_mutex_t freepool_lock;
    unsigned long cap_nr;          /* capacity. nr of elements at *elements */
    unsigned long curr_nr;         /* Current nr of elements at *elements */
    void **elements;
    int init_done;
}freepool_t;

struct fsdata fs_stat;
struct directory *root , *lastdir;
struct metadata *file;


struct block *blocks;
int nblocks;

// test
pid_t testtid = -1;
pthread_mutex_t freeblock_lock;
pthread_spinlock_t refcount_lock;

freepool_t *req_freepool;
//freepool_t *sem_freepool;

void write_done(void *private_data)
{
    struct urdmabox_req *req = (struct urdmabox_req *)private_data;
    //DEBUG_LOG(LOG_NOTICE,"write_done is called ref_count:%d\n", *(req->ref_count));
    pthread_spin_lock(&refcount_lock);
    *(req->ref_count) = *(req->ref_count) - 1;
    //DEBUG_LOG(LOG_NOTICE,"write_done decrease ref_count:%d\n", *(req->ref_count));
    if( *(req->ref_count) == 0){
	//DEBUG_LOG(LOG_NOTICE,"write_done sem_post ref_count:%d\n", *(req->ref_count));
	sem_post(req->sem);
    }
    pthread_spin_unlock(&refcount_lock);
    put_req_freepool(req_freepool, req);
}

void read_done(void *private_data)
{
    struct urdmabox_req *req = (struct urdmabox_req *)private_data;
    DEBUG_LOG(LOG_NOTICE,"read_done is called ref_count:%d\n", *(req->ref_count));
    pthread_spin_lock(&refcount_lock);
    *(req->ref_count) = *(req->ref_count) - 1;
    //DEBUG_LOG(LOG_NOTICE,"read_done decrease ref_count:%d\n", *(req->ref_count));
    if( *(req->ref_count) <= 0){
	DEBUG_LOG(LOG_NOTICE,"read_done buf:%p sem_post:%p ref_count:%d\n",req->buf, req->sem, *(req->ref_count));
	sem_post(req->sem);
    }
    pthread_spin_unlock(&refcount_lock);
    put_req_freepool(req_freepool, req);
}

void blocks_init()
{
    int i;
    for (i = 0; i < nblocks; i++) {
	blocks[i].inuse = UNUSED;
	blocks[i].next_block = -1;
    }

    pthread_mutex_init(&freeblock_lock,NULL);
}

int get_free_block_index()
{
    int i;
    //pthread_mutex_lock(&freeblock_lock);
    for (i = 0; i < nblocks; i++) {
	if ( blocks[i].inuse == UNUSED ){
	    blocks[i].inuse = USED;
	    //pthread_mutex_unlock(&freeblock_lock);
	    return i;
	}
    }
    //pthread_mutex_unlock(&freeblock_lock);
    //perror("No more blocks left!!");
    return -1;
}

/*
   void put_sem_freepool(freepool_t *pool, sem_t *element)
   {
   pthread_mutex_lock(&pool->freepool_lock);
   if(element != NULL){
   pool->elements[pool->curr_nr++] = element;
   }
   pthread_mutex_unlock(&pool->freepool_lock);
   }

   void get_sem_freepool(freepool_t *pool, sem_t **element)
   {
   if (pool->curr_nr == 0){
 *element = NULL;
 return;
 }
 pthread_mutex_lock(&pool->freepool_lock);
 if (pool->curr_nr > 0) {
 *element = pool->elements[--pool->curr_nr];
 }else{
 *element = NULL;
 }
 pthread_mutex_unlock(&pool->freepool_lock);

 return;
 }

 int sem_freepool_create(struct freepool_s **cpupool, unsigned long cap_nr)
 {   
 int i; 
 struct freepool_s *pool;
 sem_t *req;


 pool = (struct freepool_s *)malloc(sizeof(freepool_t));
 if (!pool){
 syslog(LOG_NOTICE,"mempool[%s]: Fail to create pool  \n",__func__);
 return 1;
 }
 *cpupool = pool;
 pthread_mutex_init(&pool->freepool_lock,NULL);
 pool->init_done = 0;

 pool->elements = malloc(cap_nr * sizeof(void *));
 if (!pool->elements) {
 syslog(LOG_NOTICE,"mempool[%s]: Fail to create pool elements  \n",__func__);
 free(pool);
 return 1;
 }
 pool->cap_nr = cap_nr;
 pool->curr_nr = 0;

 while (pool->curr_nr < pool->cap_nr) {
 req = (sem_t *)malloc(sizeof(sem_t));
 if (!req) {
 syslog(LOG_NOTICE,"rdmabox[%s]: req malloc failed\n",__func__);
 free(req);
 return 1;
 }
 sem_init(req, 0, 0);
 put_req_freepool(pool,req);
 }

 pool->init_done = 1;

 syslog(LOG_NOTICE,"mempool[%s]: freepool create done. curr_nr=%ld \n",__func__,pool->curr_nr);
 return 0;
 }
 */

void put_req_freepool(freepool_t *pool, struct urdmabox_req *element)
{
    pthread_mutex_lock(&pool->freepool_lock);
    if(element != NULL){
	pool->elements[pool->curr_nr++] = element;
    }
    pthread_mutex_unlock(&pool->freepool_lock);
}

void get_req_freepool(freepool_t *pool, struct urdmabox_req **element)
{
    if (pool->curr_nr == 0){
	*element = NULL;
	return;
    }
    pthread_mutex_lock(&pool->freepool_lock);
    if (pool->curr_nr > 0) {
	*element = pool->elements[--pool->curr_nr];
    }else{
	*element = NULL;
    }
    pthread_mutex_unlock(&pool->freepool_lock);

    return;
}

int req_freepool_create(struct freepool_s **cpupool, unsigned long cap_nr)
{   
    int i; 
    struct freepool_s *pool;
    struct urdmabox_req *req;


    pool = (struct freepool_s *)malloc(sizeof(freepool_t));
    if (!pool){
	syslog(LOG_NOTICE,"mempool[%s]: Fail to create pool  \n",__func__);
	return 1;
    }
    *cpupool = pool;
    pthread_mutex_init(&pool->freepool_lock,NULL);
    pool->init_done = 0;

    pool->elements = malloc(cap_nr * sizeof(void *));
    if (!pool->elements) {
	syslog(LOG_NOTICE,"mempool[%s]: Fail to create pool elements  \n",__func__);
	free(pool);
	return 1;
    }
    pool->cap_nr = cap_nr;
    pool->curr_nr = 0;

    while (pool->curr_nr < pool->cap_nr) {
	req = (struct urdmabox_req *)malloc(sizeof(struct urdmabox_req));
	//req->ref_count = 0;
	if (!req) {
	    syslog(LOG_NOTICE,"rdmabox[%s]: req malloc failed\n",__func__);
	    free(req);
	    return 1;
	}
	put_req_freepool(pool,req);
    }

    pool->init_done = 1;

    syslog(LOG_NOTICE,"mempool[%s]: freepool create done. curr_nr=%ld \n",__func__,pool->curr_nr);
    return 0;
}

/*
 ** Function to get directory path and filename relative to the directory.
 */

void get_dirname_filename ( const char *path, char *dirname, char* fname ) {

    char *ptr;

    ptr = strrchr(path, '/');

    memset(dirname, 0, NAME_MAX);
    memset(fname, 0, NAME_MAX);

    strncpy(dirname, path, (ptr - path) );
    strcpy(fname, ptr+1);

}
/*
   static void update_times(struct metadata *node, int which) {
   time_t now = time(0);
   if(which & U_ATIME) node->vstat.st_atime = now;
   if(which & U_CTIME) node->vstat.st_ctime = now;
   if(which & U_MTIME) node->vstat.st_mtime = now;
   }
 */
/* 
 ** Fill the metadata information for the file when created
 ** Accordingly add an entry into the directory structure.
 */
int fill_file_data(char *dirname, char* fname) {

    struct list *flist;
    struct directory *dir = root;
    int i;
    int ret = 0;
    int size = (int)sizeof(struct list);

    for( i = 0; i < fs_stat.max_no_of_files; i++ ) {
	if ( file[i].inuse == 0 )
	    break;
    }

    /* Fill the metadata info */

    file[i].inode = i;
    file[i].size = 0;
    file[i].data = NULL;
    file[i].inuse = USED;
    file[i].mode = S_IFREG | 0777;
    file[i].accesstime = time(NULL);
    file[i].modifiedtime = time(NULL);
    file[i].start_block = -1;
    file[i].pos_new_block = 0;
    file[i].blk_list = (int*) malloc(nblocks*sizeof(int));
    file[i].uid = getuid();
    file[i].gid = getgid();
    //update_times(&file[i], U_ATIME | U_MTIME | U_CTIME);

    /* Add an entry into directory */

    flist = (struct list *) malloc(size);
    if ( flist == NULL ) {
	perror("malloc:");
	return -ENOMEM;
    }
    strcpy(flist->fname, fname);
    flist->inode = i;
    flist->next = NULL;


    while( dir != NULL ) {
	if ( strcmp(dirname, dir->name) == 0 )
	    break;
	dir = dir->next;
    }

    file [dir->inode].accesstime = time(NULL);
    file [dir->inode].modifiedtime = time(NULL);


    if ( dir->ptr ==  NULL ) {
	dir->ptr = flist;
	dir->lptr = flist;
    }
    else {
	dir->lptr->next = flist;
	dir->lptr = flist;
    }


    fs_stat.free_bytes = fs_stat.free_bytes - size;
    fs_stat.used_bytes = fs_stat.used_bytes + size;
    fs_stat.avail_no_of_files--;

    return ret;
}


/* 
 ** Fill the metadata information for the directory when created
 ** Add a directory entry.
 */

int fill_directory_data( char *dirname, char *fname ) {

    struct directory *dir = root;
    struct list *flist;
    struct directory *newdir;
    int ret = 0;
    int i;
    int dir_size = (int) sizeof (struct directory);
    int file_size = (int) sizeof (struct list);

    for( i = 0; i < fs_stat.max_no_of_files; i++ ) {
	if ( file[i].inuse == 0 )
	    break;
    }

    /* Fill the metadata info */

    file[i].inode = i;
    file[i].size = 0;
    file[i].data = NULL;
    file[i].inuse = USED;
    file[i].mode = S_IFDIR | 0777;
    file[i].accesstime = time(NULL);
    file[i].modifiedtime = time(NULL);
    file[i].uid = getuid();
    file[i].gid = getgid();
    //update_times(&file[i], U_ATIME | U_MTIME | U_CTIME);

    /* Allocate and Populate the directory structure */

    newdir = (struct directory *) malloc(dir_size);
    if ( newdir == NULL ) {
	perror("malloc:");
	return -ENOMEM;
    }
    strcpy(newdir->name, dirname);

    if ( strcmp(dirname, "/") != 0 )
	strcat(newdir->name, "/");

    strcat(newdir->name, fname);
    newdir->inode = i;
    newdir->next = NULL;
    newdir->ptr =  NULL;
    newdir->lptr = NULL;

    /* Add an entry into directory */

    flist = (struct list *) malloc(file_size);
    if ( flist == NULL ) {
	perror("malloc:");
	return -ENOMEM;
    }
    strcpy(flist->fname, fname);
    flist->inode = i;
    flist->next = NULL;

    while( dir != NULL ) {
	if ( strcmp(dirname, dir->name) == 0 )
	    break;
	dir = dir->next;
    }

    file [dir->inode].accesstime = time(NULL);
    file [dir->inode].modifiedtime = time(NULL);
    if ( dir->ptr ==  NULL ) {
	dir->ptr = flist;
	dir->lptr = flist;
    }
    else {
	dir->lptr->next = flist;
	dir->lptr = flist;
    }

    lastdir->next = newdir;
    lastdir = newdir;

    fs_stat.free_bytes = fs_stat.free_bytes - dir_size - file_size ;
    fs_stat.used_bytes = fs_stat.used_bytes + dir_size + file_size ;
    fs_stat.avail_no_of_files--;

    return ret;

}



static void* imfs_init(struct fuse_conn_info *conn) {

    unsigned long metadata_size;
    int ret = 0;

    DEBUG_LOG(LOG_NOTICE, "[%s] \n",__func__);
    /*--------------------------------------------------------- 
      Initialize the File System structure.

      Metadata size will be MSIZE percent of total size of FS 
      ----------------------------------------------------------*/
    blocks = (struct block *) malloc(nblocks*sizeof(struct block));
    blocks_init();

    metadata_size = fs_stat.total_size * MSIZE / 100  ; 
    fs_stat.max_no_of_files = metadata_size / sizeof ( struct metadata );
    fs_stat.avail_no_of_files = fs_stat.max_no_of_files - 1;
    fs_stat.free_bytes = fs_stat.total_size - metadata_size - sizeof ( struct directory );
    fs_stat.used_bytes = sizeof ( struct directory );

    syslog(LOG_NOTICE,"capable:%lu, capable:%x \n", conn->capable, conn->capable);

#if FUSE_USE_VERSION > 29
    // disable FUSE writeback cache
    //conn->want &= ~FUSE_CAP_WRITEBACK_CACHE;

    if(conn->capable & FUSE_CAP_WRITEBACK_CACHE)
	syslog(LOG_NOTICE,"FUSE_CAP_WRITEBACK_CACHE is on\n");
    else
	syslog(LOG_NOTICE,"FUSE_CAP_WRITEBACK_CACHE is off\n");
#endif

    // Async Read improves perf. Never disable.
    //conn->want &= ~FUSE_CAP_ASYNC_READ;

    if(conn->capable & FUSE_CAP_ASYNC_READ)
	syslog(LOG_NOTICE,"FUSE_CAP_ASYNC_READ is on\n");
    else
	syslog(LOG_NOTICE,"FUSE_CAP_ASYNC_READ is off\n");

#if FUSE_USE_VERSION < 30
    if(conn->capable & FUSE_CAP_BIG_WRITES)
	syslog(LOG_NOTICE,"FUSE_CAP_BIG_WRITES is on\n");
    else
	syslog(LOG_NOTICE,"FUSE_CAP_BIG_WRITES is off\n");
#endif

    if(conn->capable & FUSE_CAP_SPLICE_WRITE)
	syslog(LOG_NOTICE,"FUSE_CAP_SPLICE_WRITE is on\n");
    else
	syslog(LOG_NOTICE,"FUSE_CAP_SPLICE_WRITE is off\n");

    if(conn->capable & FUSE_CAP_SPLICE_MOVE)
	syslog(LOG_NOTICE,"FUSE_CAP_SPLICE_MOVE is on\n");
    else
	syslog(LOG_NOTICE,"FUSE_CAP_SPLICE_MOVE is off\n");

    if(conn->capable & FUSE_CAP_SPLICE_READ)
	syslog(LOG_NOTICE,"FUSE_CAP_SPLICE_READ is on\n");
    else
	syslog(LOG_NOTICE,"FUSE_CAP_SPLICE_READ is off\n");


    syslog(LOG_NOTICE,"max_no_of_files: %d\n", fs_stat.max_no_of_files);
    syslog(LOG_NOTICE,"avail_no_of_files: %d\n", fs_stat.avail_no_of_files);

    root = ( struct directory *) malloc ( sizeof ( struct directory ) );
    if ( root == NULL) {
	perror("malloc:");
	exit(-1);
    }

    strcpy(root->name, "/");
    root->inode = 0;
    root->ptr  =  NULL;
    root->lptr =  NULL;
    root->next =  NULL;

    lastdir = root;

    file = (struct metadata *) calloc ( fs_stat.max_no_of_files, sizeof ( struct metadata ) );

    if (file == NULL) {
	perror("malloc:");
	exit(-1);
    }
    file [ROOT].inode = 0;
    file [ROOT].size = 0;
    file [ROOT].data = NULL;
    file [ROOT].inuse = 1;
    file [ROOT].mode = S_IFDIR | 0777;
    file [ROOT].accesstime = time(NULL);
    file [ROOT].modifiedtime = time(NULL);
    file [ROOT].start_block = -1;
    file [ROOT].pos_new_block = 0;
    file [ROOT].blk_list = (int*) malloc(nblocks*sizeof(int));
    file [ROOT].uid = getuid();
    file [ROOT].gid = getgid();
    syslog(LOG_NOTICE,"uid:%d gid:%d\n", file [ROOT].uid, file [ROOT].gid);

#ifdef USE_RDMA
    printf("start init rdmabox\n");
    init("100.100.100.91", 9999, "/users/jbae91/rdmabox/userspace/rdmabox/portal", nblocks);
    printf("init rdmabox done\n");

    registercallback("write_done",write_done);
    registercallback("read_done",read_done);
#endif
    return 0;

}

static int imfs_getattr(const char *path, struct stat *stbuf) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    char dirname [PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist;
    int isexists = 0;
    int index = 0;

    struct fuse_context* fc = fuse_get_context();

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);
    memset(stbuf, 0, sizeof ( struct stat ) );

    strcpy(dirname, path);
    if ( strcmp(path, "/") != 0 ) { 
	get_dirname_filename ( path, dirname, fname );
	if ( strlen(dirname) == 0 && strlen(fname) != 0 )
	    strcpy(dirname, "/");

	while ( dir != NULL ) {

	    if ( strcmp(dir->name, dirname) == 0 ) {
		flist = dir->ptr;
		while ( flist != NULL && strlen(fname) != 0 ) {
		    if ( strcmp(flist->fname, fname) == 0 ) {
			isexists = 1;
			index = flist->inode;
			break;
		    }
		    flist = flist->next;
		}

		break;
	    }

	    dir = dir->next;
	}

    }
    else{ 
	isexists = 1;
    }

    if ( !isexists ) {
	return -ENOENT;
    }

    if ( S_ISDIR ( file [index].mode ) ) {
	stbuf->st_nlink = 2; 
	stbuf->st_blocks = 1;
	stbuf->st_size = BLOCK_SIZE;
	stbuf->st_atime = file [index].accesstime;
	stbuf->st_mtime = file [index].modifiedtime;
	stbuf->st_blksize = BLOCK_SIZE;
    }
    else {
	stbuf->st_nlink = 1;
	stbuf->st_blocks = file [index].size;
	stbuf->st_size =  file [index].size;
	stbuf->st_atime = file [index].accesstime;
	stbuf->st_mtime = file [index].modifiedtime;
	stbuf->st_blksize = BLOCK_SIZE;
    }
    stbuf->st_mode = file [index].mode;
    stbuf->st_uid = file [index].uid;
    stbuf->st_gid = file [index].gid;

    return ret;
}


static int imfs_statfs(const char *path, struct statvfs *stbuf) {

    int res;

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    memset(stbuf, 0, sizeof ( struct statvfs ) );

    stbuf->f_bsize = 1;
    stbuf->f_frsize = 1;
    stbuf->f_blocks = fs_stat.total_size;
    stbuf->f_bfree = fs_stat.free_bytes;
    stbuf->f_files = fs_stat.max_no_of_files;
    stbuf->f_ffree = fs_stat.avail_no_of_files;
    stbuf->f_namemax = NAME_MAX;
    stbuf->f_bavail = fs_stat.free_bytes;

    return 0;
}


int imfs_utime(const char *path, struct utimbuf *ubuf) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist;
    int isexists = 0;

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    get_dirname_filename ( path, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0  ) {
	if ( fname == NULL )
	    return -EISDIR;
	else
	    strcpy(dirname, "/");
    }

    while ( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 ) {
	    flist = dir->ptr;
	    while ( flist != NULL ) {
		if ( strcmp(flist->fname, fname) == 0 ) {
		    isexists = 1;
		    break;
		}
		flist = flist->next;
	    }
	    break;
	}
	dir = dir->next;
    }

    if ( isexists == 0)
	return -ENOENT;


#if FUSE_USE_VERSION < 30
    ubuf->actime = file [flist->inode].accesstime;
    ubuf->modtime = file [flist->inode].modifiedtime;
#endif

    return ret;
}

/* Create a regular file */

static int imfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    char dirname [PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist;

    if ( fs_stat.avail_no_of_files == 0 || fs_stat.free_bytes < sizeof(struct list) ){
	syslog(LOG_NOTICE, "Cannot create file: No space left.");
	return -ENOSPC;
    }

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    get_dirname_filename ( path, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0 ) {
	if ( fname == NULL )
	    return -EISDIR;
	else {
	    strcpy(dirname, "/");
	}
    }

    while ( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 ) {
	    flist = dir->ptr;
	    while ( flist != NULL ) {
		if ( strcmp(flist->fname, fname) == 0 )
		    return -EEXIST;
		flist = flist->next;
	    }
	}	
	dir = dir->next;
    }

    ret = fill_file_data( dirname, fname );

    return ret;
}


/* Create a directory */

static int imfs_mkdir(const char *path, mode_t mode) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    char dirname [PATH_MAX];
    char Path [PATH_MAX]; 
    char fname [NAME_MAX];
    struct directory *dir = root;
    int ret = 0;

    if ( fs_stat.avail_no_of_files == 0 || fs_stat.free_bytes < ( sizeof(struct list) + sizeof(struct directory) ) ){
	syslog(LOG_NOTICE, "Cannot create file: No space left.");
	return -ENOSPC;
    }

    memset(Path, 0, PATH_MAX);
    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    strcpy(Path, path);

    /* Remove the last character if it is "/" */

    if ( path [strlen(path) - 1] == '/' && strlen(path) > 1 ) {
	Path [strlen(path) - 1] = '\0';
    }

    while( dir != NULL ) {
	if ( strcmp(dir->name, Path) == 0 )
	    break;
	dir = dir->next;
    }

    if ( dir != NULL )
	return -EEXIST;

    get_dirname_filename ( Path, dirname, fname );

    if ( strlen(dirname) == 0 && strlen(fname) != 0 )
	strcpy(dirname, "/");

    ret = fill_directory_data(dirname, fname);	

    return ret;

}


static int imfs_open(const char *path, struct fuse_file_info *fi) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    //syslog(LOG_NOTICE,"writepage:%d\n", fi->writepage);
    //syslog(LOG_NOTICE,"direct_io:%d\n", fi->direct_io);

    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist;
    int isexists = 0;

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    get_dirname_filename ( path, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0 ) {
	if ( fname == NULL )
	    return -EISDIR;
	else
	    strcpy(dirname, "/");
    }

    while ( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 ) {
	    flist = dir->ptr;
	    while ( flist != NULL ) {
		if ( strcmp(flist->fname, fname) == 0 ) {
		    isexists = 1;
		    break;
		}
		flist = flist->next;
	    }
	    break;
	}

	dir = dir->next;
    }

    if ( isexists == 0 )
	return -ENOENT;

    return 0;
}


static int imfs_release(const char *path, struct fuse_file_info *fi) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;

    return 0;

}

static int imfs_truncate(const char *path, off_t offset ) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    unsigned long old_size = 0;
    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist;
    int isexists = 0;

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    get_dirname_filename ( path, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0 ) {
	if ( fname == NULL )
	    return -EISDIR;
	else
	    strcpy(dirname, "/");
    }

    while ( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 ) {
	    flist = dir->ptr;
	    while ( flist != NULL ) {
		if ( strcmp(flist->fname, fname) == 0 ) {
		    isexists = 1;
		    break;
		}
		flist = flist->next;
	    }
	    break;
	}

	dir = dir->next;
    }

    if ( isexists == 0 )
	return -ENOENT;

    old_size = file [flist->inode].size;

    if ( offset == 0 ) {

	free(file [flist->inode].data);
	file [flist->inode].data = NULL;
	file [flist->inode].size = 0;
	fs_stat.free_bytes = fs_stat.free_bytes + old_size ;
	fs_stat.used_bytes = fs_stat.used_bytes - old_size ;
    }
    else {

	file [flist->inode].data = (char *) realloc( file[flist->inode].data, offset + 1);
	file [flist->inode].size = offset + 1;
	fs_stat.free_bytes = fs_stat.free_bytes + old_size - offset + 1;
	fs_stat.used_bytes = fs_stat.used_bytes - old_size + offset + 1;
    }

    return ret;
}



static int imfs_opendir(const char *path, struct fuse_file_info *fi) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    char dirname[PATH_MAX];
    struct directory *dir = root;

    strcpy(dirname, path);

    /* Remove the last character if it is "/" */

    if ( dirname [strlen(dirname) - 1] == '/'  && strlen(dirname) > 1 )
	dirname [strlen(dirname) - 1] = '\0';

    while( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 )
	    break;
	dir = dir->next;
    }

    if ( dir == NULL )
	return -ENOENT;

    return ret;
}



static int imfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    char dirname[PATH_MAX];
    struct directory *dir = root;
    struct list *flist;

    (void) offset;
    (void) fi;

    strcpy(dirname, path);

    /* Remove the last character if it is "/" */

    if ( dirname [strlen(dirname) - 1] == '/'  && strlen(dirname) > 1 )
	dirname [strlen(dirname) - 1] = '\0';

    while( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 )
	    break;
	dir = dir->next;
    }

    if ( dir == NULL )
	return -ENOENT;

#if FUSE_USE_VERSION < 30
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
#else
    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);
#endif
    flist = dir->ptr;

    while ( flist != NULL ) {
#if FUSE_USE_VERSION < 30
	filler(buf, flist->fname, NULL, 0);
#else
	filler(buf, flist->fname, NULL, 0, 0);
#endif
	flist = flist->next;
    }

    file [dir->inode].accesstime = time(NULL);

    return ret;
}

int imfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
//static int imfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist;
    int isexists = 0;
    int size2;
    off_t offset2;
    int fileblock;
#ifdef USE_RDMA
    sem_t sem_read;
    sem_init(&sem_read, 0, 0);
    int ref_count=0;
    struct urdmabox_req *req;
    /*
sem_retry:
get_sem_freepool(sem_freepool, &sem_read);
if (!sem_read){
syslog(LOG_NOTICE,"rdmabox[%s]: try to get sem \n",__func__);
goto sem_retry;
}
     */
#endif

    pid_t x = syscall(__NR_gettid);
    //int pid = getpid();
    //DEBUG_LOG(LOG_NOTICE, "[%s][%d][%d] path:%s buf:%p size:%lu, offset:%lu \n",__func__,x, pid, path, buf, size, offset);

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    get_dirname_filename ( path, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0  ) {
	if ( fname == NULL )
	    return -EISDIR;
	else
	    strcpy(dirname, "/");
    }

while ( dir != NULL ) {
    if ( strcmp(dir->name, dirname) == 0 ) {
	flist = dir->ptr;
	while ( flist != NULL ) {
	    if ( strcmp(flist->fname, fname) == 0 ) {
		isexists = 1;
		break;
	    }
	    flist = flist->next;
	}
	break;
    }
    dir = dir->next;
}

if ( isexists == 0)
    return -ENOENT;

    fileblock = file[flist->inode].start_block;

    if ( fileblock == -1 ){
	syslog(LOG_NOTICE, "read. fail to read start block buf:%p size:%lu, cur_offset:%lu \n", buf, size, offset);
	return 0;
    }

//DEBUG_LOG(LOG_NOTICE, "[%s] path:%s buf:%p size:%lu, offset:%lu, inode:%d, start fileblock:%d \n",__func__,path, buf, size, offset, flist->inode, fileblock);
//seek to the offset with fileblock pointer
while ( offset >= BLOCK_SIZE ) {
    fileblock = blocks[fileblock].next_block;
    offset -= BLOCK_SIZE;
}

//DEBUG_LOG(LOG_NOTICE, "[%s] after seeking path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size, offset, flist->inode, fileblock);

if( offset + size > BLOCK_SIZE ){
    size2 = (BLOCK_SIZE - offset);

#ifdef USE_RDMA

#if defined USE_BATCH_READ
retry:
    get_req_freepool(req_freepool, &req);
    if (!req){
	syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	goto retry;
    }
    req->block_index = fileblock;    
    req->buf = buf;
    req->offset = offset;
    req->size = size2;
    req->sem = &sem_read;
    ref_count++;
    req->ref_count = &ref_count;
    req->private_data = req;
    DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);

    put_rd_req_item(req);

    //DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);
#else
retry:
    get_req_freepool(req_freepool, &req);
    if (!req){
	syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	goto retry;
    }
    req->block_index = fileblock;    
    req->buf = buf;
    req->offset = offset;
    req->size = size2;
    req->sem = &sem_read;
    ref_count++;
    req->ref_count = &ref_count;
    req->private_data = req;
//DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);
    DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);

    read_remote_direct(req);
    sem_wait(&sem_read);
#endif

#else // USE_RDMA
    memcpy(buf, blocks[fileblock].data + offset , size2);
    DEBUG_LOG(LOG_NOTICE, "[%s] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size2, offset, flist->inode, fileblock);
#endif // USE_RDMA

    // read remaining data from next block
    offset2 = size2;
    size2 = size - size2;
    while ( size2 > 0 ) {
	fileblock = blocks[fileblock].next_block;
#ifdef USE_RDMA

#if defined USE_BATCH_READ
retry1:
	get_req_freepool(req_freepool, &req);
	if (!req){
	    syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	    goto retry1;
	}
	req->block_index = fileblock;
	req->buf = buf + offset2;
	req->offset = 0;
	req->size = size2;
	req->sem = &sem_read;
	ref_count++;
	req->ref_count = &ref_count;
	req->private_data = req;
	DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);
	//DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);

	put_rd_req_item(req);
#else
	read_remote_direct(req);
	sem_wait(&sem_read);
#endif

#else // USE_RDMA
	memcpy(buf + offset2, blocks[fileblock].data, size2);
	DEBUG_LOG(LOG_NOTICE, "[%s] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size2, 0, flist->inode, fileblock);
#endif // USE_RDMA
	size2 -= BLOCK_SIZE;
    }

}else{

#ifdef USE_RDMA

#if defined USE_BATCH_READ
retry2:
    get_req_freepool(req_freepool, &req);
    if (!req){
	syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	goto retry2;
    }
    req->block_index = fileblock;
    req->buf = buf;
    req->offset = offset;
    req->size = size;
    req->sem = &sem_read;
    ref_count++;
    req->ref_count = &ref_count;
    req->private_data = req;
    DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] blk sem:%p path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),&sem_read, path, buf, size2, offset, flist->inode, fileblock);
    //DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);

    put_rd_req_item(req);
#else
retry2:
    get_req_freepool(req_freepool, &req);
    if (!req){
	syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	goto retry2;
    }
    req->block_index = fileblock;
    req->buf = buf;
    req->offset = offset;
    req->size = size;
    req->sem = &sem_read;
    ref_count++;
    req->ref_count = &ref_count;
    req->private_data = req;
    //DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);
    DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);

    read_remote_direct(req);
    sem_wait(&sem_read);
#endif

#else
    memcpy(buf, blocks[fileblock].data + offset , size);
    DEBUG_LOG(LOG_NOTICE, "[%s] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size, offset, flist->inode, fileblock);
#endif // USE_RDMA
}

#ifdef USE_RDMA
#ifdef USE_BATCH_READ
batch_read();
    DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] wait sem:%p blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),&sem_read, path, buf, size2, offset, flist->inode, fileblock);
sem_wait(&sem_read);
    DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] done sem:%p blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),&sem_read, path, buf, size2, offset, flist->inode, fileblock);
#endif
#endif // USE_RDMA

return size;
}

int imfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    char *data_chunk;
    struct directory *dir = root;
    struct list *flist;
    int isexists = 0;
    unsigned long old_size = 0;
    int fileblock;
    int size2;
    off_t offset2;
    int last = -1;
#ifdef USE_RDMA
#ifndef USE_RDMABOX_WRITEBACK
    sem_t sem_write;
    sem_init(&sem_write, 0, 0);
    int ref_count=0;
    struct urdmabox_req *req;
#endif
#endif

    //pid_t x = syscall(__NR_gettid);
    //int pid = getpid();
    //DEBUG_LOG(LOG_NOTICE, "[%s][%d][%d] path:%s buf:%p size:%lu, offset:%lu\n",__func__,x, pid, path, buf, size, offset);

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    get_dirname_filename ( path, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0  ) {
	if ( fname == NULL )
	    return -EISDIR;
	else
	    strcpy(dirname, "/");
    }

    while ( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 ) {
	    flist = dir->ptr;
	    while ( flist != NULL ) {
		if ( strcmp(flist->fname, fname) == 0 ) {
		    isexists = 1;
		    break;
		}
		flist = flist->next;
	    }
	    break;
	}
	dir = dir->next;
    }

    if ( isexists == 0 )
	return -ENOENT;
    //DEBUG_LOG(LOG_NOTICE, "[%s] path:%s buf:%p size:%lu, offset:%lu inode:%d \n",__func__,path, buf, size, offset, flist->inode);

    fileblock = file[flist->inode].start_block;
    file [flist->inode].size = offset + size;

    if ( fileblock == -1 && size > 0) {

	fileblock = get_free_block_index();
	if (fileblock == -1) {
	    syslog(LOG_NOTICE, "Cannot write to file: No space left.");
	    return -EDQUOT;
	}
	file[flist->inode].start_block = fileblock;
	//DEBUG_LOG(LOG_NOTICE, "write new block buf:%p size:%lu, offset:%lu filesize: %lu start_block:%d\n", buf, size, offset, file[flist->inode].size,file[flist->inode].start_block);
    }else {

	//DEBUG_LOG(LOG_NOTICE, "write existing block buf:%p size:%lu, offset:%lu start fileblock:%d\n", buf, size, offset, fileblock);
	last = fileblock;

	//seek to the offset with fileblock pointer
	while ( offset >= BLOCK_SIZE ) {
	    fileblock = blocks[fileblock].next_block;
	    //DEBUG_LOG(LOG_NOTICE, "write seeking block buf:%p size:%lu, offset:%lu start fileblock:%d\n", buf, size, offset, fileblock);
	    if( fileblock != -1){
		last = fileblock;
	    }	    
	    offset -= BLOCK_SIZE;
	}

	//DEBUG_LOG(LOG_NOTICE, "write after seeking block buf:%p size:%lu, offset:%lu start last fileblock:%d\n", buf, size, offset, last);

	if ( fileblock == -1 ) {
	    fileblock = get_free_block_index();
	    if (fileblock == -1) {
		syslog(LOG_NOTICE, "Cannot write to file: No space left.");
		errno = -EDQUOT;
		return 0;
	    }

	    blocks[last].next_block = fileblock;
	    //file[flist->inode].blk_list[++file[flist->inode].pos_new_block] = fileblock;
	}

	//DEBUG_LOG(LOG_NOTICE, "write alloc block buf:%p size:%lu, offset:%lu fileblock:%d\n", buf, size, offset, fileblock);
    }

    if( offset + size > BLOCK_SIZE ){
	size2 = (BLOCK_SIZE - offset);

	// write current block
#ifdef USE_RDMA

#if defined USE_RDMABOX_WRITEBACK
	//DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);
	DEBUG_LOG(LOG_NOTICE, "[%s] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size2, offset, flist->inode, fileblock);

	//radix_write(req);
	//put_req_freepool(req_freepool, req);
	radix_write(buf, size2, offset, fileblock);

#elif defined USE_BATCH_WRITE
retry:
	get_req_freepool(req_freepool, &req);
	if (!req){
	    syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	    goto retry;
	}
	req->block_index = fileblock;    
	req->buf = buf;
	req->offset = offset;
	req->size = size2;
	req->sem = &sem_write;
	ref_count++;
	req->ref_count = &ref_count;
	req->private_data = req;
	//DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);
	//DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);

	put_wr_req_item(req);
#else
retry:
	get_req_freepool(req_freepool, &req);
	if (!req){
	    syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	    goto retry;
	}
	req->block_index = fileblock;    
	req->buf = buf;
	req->offset = offset;
	req->size = size2;
	req->sem = &sem_write;
	ref_count++;
	req->ref_count = &ref_count;
	req->private_data = req;
	//DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);
	//DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] cur path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size2, offset, flist->inode, fileblock);

	write_remote_direct(req);
	sem_wait(&sem_write);
#endif

#else // not USE_RDMA
	DEBUG_LOG(LOG_NOTICE, "[%s] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size2, 0, flist->inode, fileblock);
	memcpy(blocks[fileblock].data + offset, buf, size2);
#endif // USE_RDMA

	// write remaining data from next block
	offset2 = size2;
	size2 = size - size2;
	while ( size2 > 0 ) {
	    last = fileblock;
	    fileblock = blocks[fileblock].next_block;

	    if ( fileblock == -1 ) {
		fileblock = get_free_block_index();
		if (fileblock == -1) {
		    syslog(LOG_NOTICE, "Cannot write to file: No space left.");
		    errno = -EDQUOT;
		    return 0;
		}

		blocks[last].next_block = fileblock;
		//file[flist->inode].blk_list[++file[flist->inode].pos_new_block] = fileblock;
	    }

#ifdef USE_RDMA


#if defined USE_RDMABOX_WRITEBACK
	    //DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf + offset2, size2, 0, flist->inode, fileblock);
	    DEBUG_LOG(LOG_NOTICE, "[%s] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf + offset2, size2, 0, flist->inode, fileblock);

	    //radix_write(req);
	    //put_req_freepool(req_freepool, req);
	    radix_write(buf + offset2, size2, 0, fileblock);

#elif defined USE_BATCH_WRITE
retry1:
	    get_req_freepool(req_freepool, &req);
	    if (!req){
		syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
		goto retry1;
	    }
	    req->block_index = fileblock;
	    req->buf = buf + offset2;
	    req->offset = 0;
	    req->size = size2;
	    req->sem = &sem_write;
	    ref_count++;
	    req->ref_count = &ref_count;
	    req->private_data = req;
	    //DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf + offset2, size2, 0, flist->inode, fileblock);
	    //DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf + offset2, size2, 0, flist->inode, fileblock);

	    put_wr_req_item(req);
#else
retry1:
	    get_req_freepool(req_freepool, &req);
	    if (!req){
		syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
		goto retry1;
	    }
	    req->block_index = fileblock;
	    req->buf = buf + offset2;
	    req->offset = 0;
	    req->size = size2;
	    req->sem = &sem_write;
	    ref_count++;
	    req->ref_count = &ref_count;
	    req->private_data = req;
	    //DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf + offset2, size2, 0, flist->inode, fileblock);
	    DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf + offset2, size2, 0, flist->inode, fileblock);

	    write_remote_direct(req);
	    sem_wait(&sem_write);
#endif

#else // not USE_RDMA
	    DEBUG_LOG(LOG_NOTICE, "[%s] rem path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size2, 0, flist->inode, fileblock);
	    memcpy(blocks[fileblock].data, buf+offset2, size2);
#endif // USE_RDMA

	    size2 -= BLOCK_SIZE;
	}//while processing the rest

    }else{ // offset + size <= BLOCK_SIZE

#ifdef USE_RDMA
#if defined USE_RDMABOX_WRITEBACK
	//DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size, offset, flist->inode, fileblock);
	DEBUG_LOG(LOG_NOTICE, "[%s] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size, offset, flist->inode, fileblock);

	//radix_write(req);
	//put_req_freepool(req_freepool, req);
	radix_write(buf, size, offset, fileblock);

#elif defined USE_BATCH_WRITE
retry2:
	get_req_freepool(req_freepool, &req);
	if (!req){
	    syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	    goto retry2;
	}
	req->block_index = fileblock;
	req->buf = buf;
	req->offset = offset;
	req->size = size;
	req->sem = &sem_write;
	ref_count++;
	req->ref_count = &ref_count;
	req->private_data = req;
//DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size, offset, flist->inode, fileblock);
	//DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size, offset, flist->inode, fileblock);

	put_wr_req_item(req);
#else
retry2:
	get_req_freepool(req_freepool, &req);
	if (!req){
	    syslog(LOG_NOTICE,"rdmabox[%s]: try to get req \n",__func__);
	    goto retry2;
	}
	req->block_index = fileblock;
	req->buf = buf;
	req->offset = offset;
	req->size = size;
	req->sem = &sem_write;
	ref_count++;
	req->ref_count = &ref_count;
	req->private_data = req;
//DEBUG_LOG(LOG_NOTICE, "[%d][%s][ref_count:%d] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",x,__func__,*(req->ref_count),path, buf, size, offset, flist->inode, fileblock);
	DEBUG_LOG(LOG_NOTICE, "[%s][ref_count:%d] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,*(req->ref_count),path, buf, size, offset, flist->inode, fileblock);

	write_remote_direct(req);
	sem_wait(&sem_write);
#endif

#else // not USE_RDMA
	DEBUG_LOG(LOG_NOTICE, "[%s] blk path:%s buf:%p size:%lu, offset:%lu, inode:%d, fileblock:%d \n",__func__,path, buf, size, offset, flist->inode, fileblock);
	memcpy(blocks[fileblock].data + offset, buf, size);
#endif // USE_RDMA
    }//offset + size <= BLOCK_SIZE

#ifdef USE_RDMA

#ifdef USE_BATCH_WRITE
    batch_write();
    sem_wait(&sem_write);
#endif

#endif // USE_RDMA

    //DEBUG_LOG(LOG_NOTICE, "[%s] done buf:%p size:%lu, offset:%lu fileblock:%d, filesize: %lu\n",__func__, buf, size, offset, fileblock, file[flist->inode].size);
    return size;
}


static int imfs_unlink(const char *path) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    char *data_chunk;
    struct directory *dir = root;
    struct list *flist;
    struct list *prev;
    int isexists = 0;

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    get_dirname_filename ( path, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0  ) {
	if ( fname == NULL )
	    return -EISDIR;
	else
	    strcpy(dirname, "/");
    }

    while ( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 ) {
	    flist = dir->ptr;
	    while ( flist != NULL ) {
		if ( strcmp(flist->fname, fname) == 0 ) {
		    isexists = 1;
		    break;
		}
		prev = flist;
		flist = flist->next;
	    }
	    break;
	}
	dir = dir->next;
    }

    if ( isexists == 0 )
	return -ENOENT;

    if ( flist == dir->ptr ) {
	dir->ptr = flist->next;
	if ( dir->ptr == NULL ) 
	    dir->lptr = NULL;
    }
    else {

	prev->next = flist->next;
	if ( flist == dir->lptr )
	    dir->lptr = prev;
    }


    file [flist->inode].inuse = 0;
    free(file [flist->inode].data);
    file [flist->inode].data = NULL;

    fs_stat.free_bytes = fs_stat.free_bytes + file [flist->inode].size + sizeof ( struct list );
    fs_stat.used_bytes = fs_stat.used_bytes - file [flist->inode].size - sizeof ( struct list );
    fs_stat.avail_no_of_files++;
    file [flist->inode].size = 0;

    free(flist);
    return ret;
}


static int imfs_rmdir(const char *path) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    char Path [PATH_MAX]; 
    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct directory *prev;

    memset(Path, 0, PATH_MAX);
    strcpy(Path, path);

    /* Remove the last character if it is "/" */

    if ( path [strlen(path) - 1] == '/'  && strlen(path) > 1 ) {
	Path [strlen(path) - 1] = '\0';
    }

    while( dir != NULL ) {
	if ( strcmp(dir->name, Path) == 0 )
	    break;
	prev = dir;
	dir = dir->next;
    }

    if ( dir == NULL )
	return -ENOENT;

    if ( dir->ptr != NULL )
	return -ENOTEMPTY;

    if ( strcmp(path,"/") == 0 )
	return -EBUSY;

    ret = imfs_unlink(path);

    prev->next = dir->next;
    if ( dir == lastdir )
	lastdir = prev;
    free(dir);

    fs_stat.free_bytes = fs_stat.free_bytes + sizeof ( struct directory );
    fs_stat.used_bytes = fs_stat.used_bytes - sizeof ( struct directory );

    return ret;

} 

int imfs_rename(const char *path, const char *newpath) {

    DEBUG_LOG(LOG_NOTICE, "[%s] path:%s \n",__func__,path);

    int ret = 0;
    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist, *prev;
    int index = 0;
    int isexists = 0;

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    if ( strcmp(path, "/") == 0 )
	return -EBUSY;

    get_dirname_filename ( path, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0  ) {
	if ( fname == NULL )
	    return -EISDIR;
	else
	    strcpy(dirname, "/");
    }

    while ( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 ) {
	    flist = dir->ptr;
	    while ( flist != NULL ) {
		if ( strcmp(flist->fname, fname) == 0 ) {
		    isexists = 1;
		    index = flist->inode;
		    break;
		}
		prev = flist;
		flist = flist->next;
	    }
	    break;
	}
	dir = dir->next;
    }

    if ( isexists == 0)
	return -ENOENT;

    if ( flist == dir->ptr ) {
	dir->ptr = flist->next;
	if ( dir->ptr == NULL ) 
	    dir->lptr = NULL;
    }
    else {

	prev->next = flist->next;
	if ( flist == dir->lptr )
	    dir->lptr = prev;
    }

    free(flist);

    get_dirname_filename ( newpath, dirname, fname );

    if ( strlen(dirname) == 0 || strlen(fname) == 0  ) {
	if ( fname == NULL )
	    return -EISDIR;
	else
	    strcpy(dirname, "/");
    }

    dir = root;

    while ( dir != NULL ) {
	if ( strcmp(dir->name, dirname) == 0 ) {
	    break;
	}
	dir = dir->next;
    }

    if ( dir == NULL )
	return -ENOENT;

    flist = (struct list *) malloc(sizeof ( struct list ) );
    if ( flist == NULL) {
	perror("malloc:");
	return -ENOMEM;
    }
    strcpy(flist->fname, fname);
    flist->inode = index;
    flist->next = NULL;

    file [dir->inode].accesstime = time(NULL);
    file [dir->inode].modifiedtime = time(NULL);

    file [index].accesstime = time(NULL);
    file [index].modifiedtime = time(NULL);


    if ( dir->ptr ==  NULL ) {
	dir->ptr = flist;
	dir->lptr = flist;
    }
    else {
	dir->lptr->next = flist;
	dir->lptr = flist;
    }

    // Change the directory name.

    dir = root;

    while( dir != NULL ) {
	if (index == dir->inode)
	    break;
	dir = dir->next;
    }

    if ( dir !=  NULL ) {
	memset(dir->name, 0, PATH_MAX);
	strcpy(dir->name, newpath);
    }

    return ret;

}

static void imfs_destroy () {

    DEBUG_LOG(LOG_NOTICE, "[%s] \n",__func__);

    int i;
    struct directory *dir;
    struct list *flist;

    for( i = 0; i < fs_stat.max_no_of_files; i++ ) {
	free(file [i].data);
    }

    free(file);

    while ( root != NULL ) {
	dir = root;
	while ( dir->ptr != NULL ) {
	    flist = dir->ptr;
	    dir->ptr = dir->ptr->next;
	    free(flist);
	}
	root = root->next;
	free(dir);
    }

}

static int imfs_chmod(const char *path, mode_t mode,
	struct fuse_file_info *fi)
{
    DEBUG_LOG(LOG_NOTICE, "[%s] \n",__func__);

    int ret = 0;
    char dirname [PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist;
    int isexists = 0;
    int index = 0;

    struct fuse_context* fc = fuse_get_context();

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    strcpy(dirname, path);
    if ( strcmp(path, "/") != 0 ) { 
	get_dirname_filename ( path, dirname, fname );
	if ( strlen(dirname) == 0 && strlen(fname) != 0 )
	    strcpy(dirname, "/");

	while ( dir != NULL ) {

	    if ( strcmp(dir->name, dirname) == 0 ) {
		flist = dir->ptr;
		while ( flist != NULL && strlen(fname) != 0 ) {
		    if ( strcmp(flist->fname, fname) == 0 ) {
			isexists = 1;
			index = flist->inode;
			break;
		    }
		    flist = flist->next;
		}

		break;
	    }

	    dir = dir->next;
	}

    }
    else 
	isexists = 1;

    if ( !isexists ) {
	return -ENOENT;
    }

    mode_t mask = S_ISUID | S_ISGID | S_ISVTX |
	S_IRUSR | S_IWUSR | S_IXUSR |
	S_IRGRP | S_IWGRP | S_IXGRP |
	S_IROTH | S_IWOTH | S_IXOTH;

    file[index].mode = (file [index].mode & ~mask) | (mode & mask);

    return 0;
}

static int imfs_chown(const char *path, uid_t uid, gid_t gid,
	struct fuse_file_info *fi)
{
    DEBUG_LOG(LOG_NOTICE, "[%s] \n",__func__);

    int ret = 0;
    char dirname [PATH_MAX];
    char fname [NAME_MAX];
    struct directory *dir = root;
    struct list *flist;
    int isexists = 0;
    int index = 0;

    memset(dirname, 0, PATH_MAX);
    memset(fname, 0, NAME_MAX);

    strcpy(dirname, path);
    if ( strcmp(path, "/") != 0 ) { 
	get_dirname_filename ( path, dirname, fname );
	if ( strlen(dirname) == 0 && strlen(fname) != 0 )
	    strcpy(dirname, "/");

	while ( dir != NULL ) {

	    if ( strcmp(dir->name, dirname) == 0 ) {
		flist = dir->ptr;
		while ( flist != NULL && strlen(fname) != 0 ) {
		    if ( strcmp(flist->fname, fname) == 0 ) {
			isexists = 1;
			index = flist->inode;
			break;
		    }
		    flist = flist->next;
		}

		break;
	    }

	    dir = dir->next;
	}

    }
    else 
	isexists = 1;

    if ( !isexists ) {
	return -ENOENT;
    }

    file [index].uid = uid;
    file [index].gid = gid;

    return 0;
}

static struct fuse_operations imfs_oper = {

    .init 		= imfs_init,
    .getattr	= imfs_getattr,
    .statfs		= imfs_statfs,
#if FUSE_USE_VERSION < 30
    .utime		= imfs_utime,
#endif
    .readdir	= imfs_readdir,
    .open		= imfs_open,
    .read		= imfs_read,
    .create		= imfs_create,
    .mkdir		= imfs_mkdir,
    .opendir	= imfs_opendir,
    .release	= imfs_release,
    .write		= imfs_write,
    .rename		= imfs_rename,
    .truncate	= imfs_truncate,
    .unlink		= imfs_unlink,
    .rmdir		= imfs_rmdir,
    .chown     = imfs_chown,
    .chmod     = imfs_chmod,
    .destroy	= imfs_destroy, 

};

int main(int argc, char *argv[]) {

    int size;
    int i = 2;

    setlogmask (LOG_UPTO (LOG_NOTICE));
    openlog ("myfuse", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);


    if ( argc < 3 ) {
	printf("%s <mountpoint> <size in (MB)>\n", argv[0]);
	exit(-1);
    }

    //size = atoi(argv[2]);
    size = 30000;
    syslog(LOG_NOTICE,"Total space for storage: %lu MB\n", size);

    fs_stat.total_size = size * 1024; /* In KB */
    //syslog(LOG_NOTICE,"Total size: %lu KB\n", fs_stat.total_size);

    nblocks = fs_stat.total_size/(BLOCK_SIZE/1024);
    syslog(LOG_NOTICE,"number of blocks: %lu\n", nblocks);

    if(req_freepool_create(&req_freepool, 1024))
	syslog(LOG_NOTICE,"fail to create rdma req freepool\n");
    /*
       if(sem_freepool_create(&sem_freepool, 10240))
       syslog(LOG_NOTICE,"fail to create rdma sem freepool\n");
     */

    pthread_spin_init(&refcount_lock,0);

    /*
       while ( (i + 1) < argc ) {
       strcpy(argv[i], argv[i+1]);
       i++;
       }
       argc--;		
       argv[argc] = NULL;
     */
    return fuse_main(argc, argv, &imfs_oper, NULL);
}
