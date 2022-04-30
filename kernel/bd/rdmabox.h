/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
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
        ctx[0]->isSGE=1;
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

#ifndef RDMABOX_H
#define RDMABOX_H

#include <linux/init.h>
#include <linux/err.h>
#include <linux/string.h>
#include <linux/parser.h>
#include <linux/proc_fs.h>
#include <linux/inet.h>
#include <linux/in.h>
#include <linux/device.h>
#include <linux/pci.h>
#include <linux/time.h>
#include <linux/random.h>
#include <linux/sched.h>
#include <linux/proc_fs.h>
#include <asm/pci.h>
#include <rdma/ib_verbs.h>
#include <rdma/rdma_cm.h>

#include "rpg_drv.h"


#define USE_NEW_HYBRID_POLL 
#define MAX_POLL_WC 32
#define RETRY 4

//#define USE_MULTI_SCQ_BUSY_POLL 
//#define NUM_SCQ 1
//#define NUM_POLL_THREAD 1

//#define USE_BUSY_POLL 

//#define USE_EVENT_POLL

//#define USE_EVENT_BATCH_POLL
//#define MAX_POLL_WC 128


#define PRE_ALLOC

//#define FLOW_CONTROL

#define MAX_SGE 32 // this is for Mellanox CX-3

#define DOORBELL_BATCH_SIZE 32

//  server selection, call m server each time.
// SERVER_SELECT_NUM should be >= NUM_REPLICA
#define SERVER_SELECT_NUM 1
// NUM_REPLICA should be 1 if there is only 1 server
#define NUM_REPLICA 1

// num of channels(QPs) per remote node
#define NUM_CHANNEL 4

// MR and ctx pool
#define NUM_POOL 1
#define NUM_WR_CTX 256
#define NUM_WR_MR 64
#define NUM_RD_CTX 256
#define NUM_RD_MR 64

#define FREE_MEM_WEIGHT 1
#define GRAD_WEIGHT 1
//#define AGE_WEIGHT 1

//max_size from one server or max_size one server can provide
#define MAX_MR_SIZE_GB 48

#define MAX_PORTAL_NAME   2048

#define TRIGGER_ON 1
#define TRIGGER_OFF 0

#define NO_CB_MAPPED -1

#define CHUNK_MAPPED 1
#define CHUNK_UNMAPPED 0

#define CTX_IDLE	0
#define CTX_R_IN_FLIGHT	1
#define CTX_W_IN_FLIGHT	2

// added for RDMA_CONNECTION failure handling.
#define DEV_RDMA_ON		1
#define DEV_RDMA_OFF	0

//bitmap
//#define INT_BITS 32
#define BITMAP_SHIFT 5 // 2^5=32
#define ONE_GB_SHIFT 30
#define BITMAP_MASK 0x1f // 2^5=32
#define ONE_GB_MASK 0x3fffffff
#define ONE_GB 1073741824 //1024*1024*1024 
#define BITMAP_INT_SIZE 8192 //bitmap[], 1GB/4k/32

#define SUCCESS_ALL 0
#define SUCCESS_CTX_ONLY 1
#define SUCCESS_MR_ONLY 2
#define FAIL_ALL 3

extern struct list_head g_RDMABOX_sessions;

//TODO : remov this. use DMA only
enum mem_type {
	DMA = 1,
	FASTREG = 2,
	MW = 3,
	MR = 4
};

struct portal {
        uint8_t addr[16];                    /* dst addr in NBO */
        uint16_t port;                  /* dst port in NBO */
};

enum msg_opcode{
    DONE = 1,
    INFO,
    INFO_SINGLE,
    INFO_MIGRATION,
    QUERY,
    RESP_QUERY,
    //REQ_ACTIVITY,
    //RESP_ACTIVITY,
    EVICT,
    BIND,
    BIND_MIGRATION,
    BIND_SINGLE,
    DEST_INFO,
    REQ_MIGRATION,
    DONE_MIGRATION,
    DELETE_CHUNK
};

struct RDMABOX_rdma_info {
	enum msg_opcode type;
  	int size_gb;
        int chunk_index;
        int cb_index;
        int replica_index;
  	uint64_t buf[MAX_MR_SIZE_GB];
  	uint32_t rkey[MAX_MR_SIZE_GB];
        char reserve[3496];
};

struct query_message{
  enum msg_opcode type;
  int size_gb;
  int grad;
  unsigned long sum;
  char reserve[4080];
};


enum test_state {
	IDLE = 1,
	CONNECT_REQUEST,
	ADDR_RESOLVED,
	ROUTE_RESOLVED,
	CONNECTED,		// updated by RDMABOX_cma_event_handler()
	RECV_FREE_MEM,
	AFTER_FREE_MEM,
	RECV_INFO_SINGLE,
	RECV_EVICT,
        RECV_INFO_MIG,
        RECV_MIGRATION_DONE,
	CM_DISCONNECT,
	ERROR
};

enum chunk_state {
       ACTIVE=1,
       READONLY,
       NONE_ALLOWED,
       INACTIVE
};
/*        
struct chunk_header {
       struct timeval *tv;
       enum chunk_state state;
       u8 addr[16];
       u8 reserve[4064];
};
*/
// 1GB remote chunk struct	("chunk": we use the term "slab" in our paper)
struct remote_chunk_g {
	uint32_t remote_rkey;		/* remote guys RKEY */
	uint64_t remote_addr;		/* remote guys TO */
        enum chunk_state chunk_state;
	int *bitmap_g;	//1GB bitmap
	int replica_index;
};

enum chunk_list_state {
	C_IDLE,
	C_READY,
	C_EVICT,
	C_QUERY,
	C_MIGRATION_READY,
	C_MIGRATION_DONE,
	C_DELETE_CHUNK,
	// C_OFFLINE
};

struct remote_chunk_g_list {
	struct remote_chunk_g **chunk_list;
	struct remote_chunk_g **migration_list;
	atomic_t *remote_mapped; //atomic_t type * MAX_MR_SIZE_GB

	int chunk_size_g; //size = chunk_num * ONE_GB
	int target_size_g; // free_mem from server
	int shrink_size_g;
        //unsigned long sum; // sum of age from server
	int grad;

	int *chunk_map;	//cb_chunk_index to session_chunk_index
	struct task_struct *evict_handle_thread;
	char *evict_chunk_map;
	char *migration_chunk_map;
	wait_queue_head_t sem;      	
	enum chunk_list_state c_state;

        int dest_chunk_index;
        int dest_cb_index;
        int src_chunk_index;
        int src_cb_index;
        int client_swapspace_index; 
        int replica_index;
};


typedef struct rdma_freepool_s {
        spinlock_t rdma_freepool_lock;
        unsigned long cap_nr_ctx;          /* capacity. nr of elements at *elements */
        unsigned long curr_nr_ctx;         /* Current nr of elements at *elements */
        unsigned long cap_nr_mr;          /* capacity. nr of elements at *elements */
        unsigned long curr_nr_mr;         /* Current nr of elements at *elements */
        void **ctx_elements;
        void **mr_elements;
        int init_done;
}rdma_freepool_t;

struct conn_private_data{
  int mig_cb_index;
  u8 addr[16];
};

/*
 *  rdma kernel Control Block struct.
 *  one kernel control block for one server
 *  one queue pair for one server
 */
// TODO : this will be connection
struct kernel_cb {
	int cb_index; //index in RDMABOX_sess->cb_list
	struct RDMABOX_session *RDMABOX_sess;
	int server;			/* 0 iff client */
	struct ib_cq *cq;
	struct ib_pd *pd;
	struct ib_qp *qp[NUM_CHANNEL];
	struct ib_qp **qplist; // NUM_CHANNEL QPs are assigned to 32 qplist slots

	enum mem_type mem;
	struct ib_mr *dma_mr;

	// memory region
	struct ib_recv_wr rq_wr;	/* recv work request record */
	struct ib_sge recv_sgl;		/* recv single SGE */
	struct RDMABOX_rdma_info recv_buf;/* msg buffer. malloc'd buffer */ 
	u64 recv_dma_addr;
	DECLARE_PCI_UNMAP_ADDR(recv_mapping)
	struct ib_mr *recv_mr;

	struct ib_send_wr sq_wr;	/* send work requrest record */
	struct ib_sge send_sgl;
	struct RDMABOX_rdma_info send_buf;/* msg buffer. single send buf */
	u64 send_dma_addr;
	DECLARE_PCI_UNMAP_ADDR(send_mapping)
	struct ib_mr *send_mr;

	struct remote_chunk_g_list remote_chunk;

	enum test_state state;		/* used for cond/signalling */
	wait_queue_head_t sem;      // semaphore for wait/wakeup
	wait_queue_head_t write_sem;      // semaphore for wait/wakeup

	// from arg
	u8 addr[16];			/* dst addr in NBO */
	uint16_t port;			/* dst port in NBO */
	uint8_t addr_type;		/* ADDR_FAMILY - IPv4/V6 */
	int verbose;			/* verbose logging */
	int size;			/* ping data size */
	int txdepth;			/* SQ depth */
	int local_dma_lkey;		/* use 0 for lkey */

	/* CM stuff  connection management*/
	struct rdma_cm_id *cm_id[NUM_CHANNEL];	/* connection on client side,*/
	struct rdma_cm_id *child_cm_id;	/* connection on client side,*/
					/* listener on server side. */

        rdma_freepool_t **rdma_write_freepool_list;
        rdma_freepool_t **rdma_read_freepool_list;

	struct list_head list;	
};

struct rdma_mr {
        char *rdma_buf;
	u64  rdma_dma_addr;
	DECLARE_PCI_UNMAP_ADDR(rdma_mapping)
        int cbhint;
        int cpuhint;
};

struct rdma_ctx {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(4, 4, 0)
        struct ib_rdma_wr rdma_sq_wr;        /* rdma work request record */
#else
        struct ib_send_wr rdma_sq_wr;        /* rdma work request record */
#endif
        struct ib_sge rdma_sgl_wr[MAX_SGE];           /* rdma single SGE */
	struct sg_table data_tbl;
        struct scatterlist sgl[MAX_SGE];
	int total_segments;

        struct rdma_mr *mr;

#if defined(USE_DOORBELL_WRITE) ||defined(USE_DOORBELL_WRITE_SGE) || defined(USE_DOORBELL_READ) || defined(USE_DOORBELL_READ_SGE)
	struct request *reqlist[DOORBELL_BATCH_SIZE];
#else
	struct request *reqlist[RDMA_WR_BUF_LEN];
#endif
/*
#if defined(NEW_HYBRID_WRITE) || defined(NEW_HYBRID_READ)
	struct rdma_ctx *ctxlist[DOORBELL_BATCH_SIZE];
#endif
*/
        int numreq;
        int isSGE;

	int chunk_index;
	struct kernel_cb *cb;
	unsigned long offset;
	unsigned long len;
	struct remote_chunk_g *chunk_ptr;
        atomic_t in_flight; //true = 1, false = 0
        int replica_index;
        struct rdma_ctx *prime_ctx;
	struct local_page_list *sending_item;
        int cbhint;
        int cpuhint;
        atomic_t ref_count;
#ifdef LAT_DEBUG
        struct timeval starttime;
#endif
};

/*
// TODO : remove this. obsolete
struct RDMABOX_connection {
	struct kernel_cb		**cbs;
	struct RDMABOX_session    *RDMABOX_sess;
	struct task_struct     *conn_th;
	int			cpu_id;
	int			wq_flag;
	wait_queue_head_t	wq;
};
*/
struct RDMABOX_portal {
	uint16_t port;			/* dst port in NBO */
	u8 addr[16];			/* dst addr in NBO */
};
enum cb_state {
	CB_IDLE=0,
	CB_CONNECTED,	//connected but not mapped 
	CB_MAPPED,
	CB_MIGRATING,
	CB_EVICTING,
	CB_FAIL
};

#if defined(USE_MULTI_SCQ_BUSY_POLL)
struct cq_thread_data {
        struct RDMABOX_session *RDMABOX_sess;
        struct ib_cq *scq;
        struct task_struct *spoll_thread;
        //atomic_t *wc_handler_on;
        struct work_cont *work_cont;
        //struct tasklet_struct *tasklet;
        int tid;
};
#endif

#ifdef USE_HYBRID_POLL
struct work_cont {
        struct work_struct real_work;
        //struct cq_thread_data *tdata;
        struct ib_cq *scq;
};
#endif

// Session is only one for one block device client
// A session has multiple connections.(as same as number of CPUs)
// each connection has ctx pool and connection has multiple ctx (on demand???)
struct RDMABOX_session {
	u8 node_addr[16];			/* dst addr in NBO */
#if defined(USE_MULTI_SCQ_BUSY_POLL)
        atomic_t curr_scq_index;
        atomic_t curr_thread_index;
        struct ib_cq *scq[NUM_SCQ];
        struct cq_thread_data tdata[NUM_SCQ];
#endif

#ifdef USE_HYBRID_POLL
        int tdata_init_done[NUM_SCQ];
        int wq_init_done[NUM_SCQ];
        struct work_cont test_wq[NUM_SCQ];
#endif
	int mapped_cb_num;	//How many cbs are remote mapped
	struct kernel_cb	**cb_list;	
	struct RDMABOX_portal *portal_list;
	int cb_num;	//num of possible servers
	enum cb_state *cb_state_list; //all cbs state: not used, connected, failure

	struct RDMABOX_file 		*xdev;	// each session only creates a single RDMABOX_file
	//struct RDMABOX_connection	    **RDMABOX_conns;

	char			      portal[MAX_PORTAL_NAME];

	struct list_head	      list;
	struct list_head	      devs_list; /* list of struct RDMABOX_file */
	spinlock_t		      devs_lock;
	spinlock_t		      cb_lock;
	struct config_group	      session_cg;
	struct completion	      conns_wait;
	atomic_t		      conns_count;
	atomic_t		      destroy_conns_count;

	unsigned long long    capacity;
	unsigned long long 	  mapped_capacity;
	int 	capacity_g;

	//atomic_t *cb_index_map;  //unmapped==-1, this chunk is mapped to which cb
	atomic_t *cb_index_map[NUM_REPLICA];  //unmapped==-1, this chunk is mapped to which cb
	//int *mapping_swapspace_to_chunk;
	int *mapping_swapspace_to_chunk[NUM_REPLICA];
	//int *unmapped_chunk_list; //TODO : need this ?
	//int free_chunk_index; //active header of unmapped_chunk_list TODO: need this ?
	atomic_t	rdma_on;	//DEV_RDMA_ON/OFF

	struct ib_pd *pd;

        //rdma_freepool_t **rdma_write_freepool;
        //rdma_freepool_t **rdma_read_freepool;

        //swap space ops counter
        int pos_wr_hist;
        int pos_rd_hist;
        unsigned long wr_ops[STACKBD_SIZE_G][3];
        unsigned long rd_ops[STACKBD_SIZE_G][3];
/*
       struct task_struct *remote_sender_thread;
       struct task_struct *direct_sender_thread;
       struct task_struct *direct_reader_thread;
*/
       struct task_struct *doorbell_batch_write_thread;
       struct task_struct *doorbell_batch_read_thread;
};

//static struct RDMABOX_session *RDMABOX_sess;

//void put_ctx_freepool(rdma_freepool_t *pool, void *element);
//void *get_ctx_freepool(rdma_freepool_t *pool);
void get_freepool(rdma_freepool_t *pool, void **ctx_element, void **mr_element, int *nctx, int *nmr);
void put_freepool(rdma_freepool_t *pool, void *ctx_element, void *mr_element);

int RDMABOX_single_chunk_map(struct RDMABOX_session *RDMABOX_session, int select_chunk, struct kernel_cb **replica_cb_list);
struct kernel_cb* RDMABOX_migration_chunk_map(struct RDMABOX_session *RDMABOX_session, int client_evict_chunk_index, struct kernel_cb *old_cb, int src_cb_index, int replica_index);

int RDMABOX_session_create(const char *portal, struct RDMABOX_session *RDMABOX_session);
void RDMABOX_session_destroy(struct RDMABOX_session *RDMABOX_session);
int RDMABOX_create_device(struct RDMABOX_session *RDMABOX_session,
		       const char *xdev_name, struct RDMABOX_file *RDMABOX_file);
void RDMABOX_destroy_device(struct RDMABOX_session *RDMABOX_session,
                         struct RDMABOX_file *RDMABOX_file);

int RDMABOX_create_configfs_files(void);
void RDMABOX_destroy_configfs_files(void);
struct RDMABOX_file *RDMABOX_file_find(struct RDMABOX_session *RDMABOX_session,
                                 const char *name);
struct RDMABOX_session *RDMABOX_session_find_by_portal(struct list_head *s_data_list,
                                                 const char *portal);
const char* RDMABOX_device_state_str(struct RDMABOX_file *dev);
int RDMABOX_set_device_state(struct RDMABOX_file *dev, enum RDMABOX_dev_state state);

void RDMABOX_single_chunk_init(struct kernel_cb *cb);
void RDMABOX_chunk_list_init(struct kernel_cb *cb);

//int remote_sender_write(struct RDMABOX_session *RDMABOX_sess, struct local_page_list *tmp, int cpuhint);
int request_write(struct RDMABOX_session *RDMABOX_sess, struct request *req, int cpuhint);
//int request_write(struct RDMABOX_session *RDMABOX_sess, struct rdmabox_msg *msg, int cpuhint);
int request_read(struct RDMABOX_session *RDMABOX_sess, struct request *req, int cpuhint);
//int request_read(struct RDMABOX_session *RDMABOX_sess, struct rdmabox_msg *msg, int cpuhint);

void RDMABOX_bitmap_set(int *bitmap, int i);
bool RDMABOX_bitmap_test(int *bitmap, int i);
void RDMABOX_bitmap_clear(int *bitmap, int i);
void RDMABOX_bitmap_init(int *bitmap);
void RDMABOX_bitmap_group_set(int *bitmap, unsigned long offset, unsigned long len);
void RDMABOX_bitmap_group_clear(int *bitmap, unsigned long offset, unsigned long len);
int isRDMAon(struct RDMABOX_session *RDMABOX_sess);

int direct_sender_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint);
int direct_reader_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint);
int remote_sender_fn(struct RDMABOX_session *RDMABOX_sess, int cpuhint);
/*
int pre_check_write(struct RDMABOX_session *RDMABOX_sess, struct request *tmp, int swapspace_index, unsigned long start_tmp, int cpuhint);
int prepare_write(struct request *tmp, int swapspace_index, struct rdma_ctx **ctx, struct kernel_cb **replica_cb_list, int cpuhint);
*/
#endif  /* RDMABOX_H */

