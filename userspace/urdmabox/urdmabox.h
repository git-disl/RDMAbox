#ifndef URDMABOX_H
#define URDMABOX_H
/*
 * Copyright (c) 2005 Ammasso, Inc. All rights reserved.
 * Copyright (c) 2006-2009 Open Grid Computing, Inc. All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#include <stdbool.h>
#include <getopt.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <pthread.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <linux/kernel.h>
#include <sys/time.h>
#include <time.h>
#include <syslog.h>

#include <endian.h>
#include <byteswap.h>

#include "bitmap.h"
#include "lfqueue.h"

#define PAGE_SIZE 4096

//For temporary debug only. Standalone test in main()
//#define NUM_BLOCK 32 

// define this to use dynamic mr registration
//#define DYNAMIC_MR_REG

#define ONE_MB 1048576
#define ONE_GB 1073741824

#define FUSE_BLOCK_SIZE 131072 // 128kb without kernel modification, typically 128kb

// below should be matched to server daemon
///////////////////////////////////////////
// ramdisk block size
#define LOCAL_BLOCK_SIZE ONE_MB
//#define LOCAL_BLOCK_SIZE 131072 // 128kb
#define REMOTE_BLOCK_SIZE ONE_GB
#define MAX_RB_NUM 32 // for c6220
//#define MAX_RB_NUM 12 // for r320
///////////////////////////////////////////

static int LOCAL_BLOCKS_PER_REMOTE_BLOCK; // REMOTE_BLOCK_SIZE/LOCAL_BLOCK_SIZE

#define RDMA_WR_BUF_LEN 10
#define RDMA_BUF_SIZE LOCAL_BLOCK_SIZE * RDMA_WR_BUF_LEN
#define NUM_MR 1024 // RDMA_BUF
#define NUM_CTX 1024

//TODO : set this num of request in a batch list
// for now dummy 10.
#define MAX_RDMA_WR_BATCH_LEN 10

//this is for writeback mode
//#define USE_RDMABOX_WRITEBACK

//  server selection, call m server each time.
// SERVER_SELECT_NUM should be >= NUM_REPLICA
#define SERVER_SELECT_NUM 1
#define NUM_REPLICA 1

//#define USE_EVENT_POLL
//#define USE_BUSY_POLL
#define USE_HYBRID_POLL
#define MAX_POLL_WC 32
#define RETRY 4

#define RPC_MSG_POOL_SIZE 1024
#define MAX_CLIENT 32
#define MAX_MSG_NUM 32 // max num of MR in rpc msg , min should be 2

#define RPING_SQ_DEPTH 16351
#define RPING_RQ_DEPTH 10240
#define CQ_DEPTH 10240

//#define POST_RECV_DEPTH 1024
#define POST_RECV_DEPTH 1

#define MAX_SGE 32

#define NO_CB_MAPPED -1
#define RB_MAPPED 1
#define RB_UNMAPPED 0

#define uint64_from_ptr(p)    (uint64_t)(uintptr_t)(p)
#define ptr_from_uint64(p)    (void *)(unsigned long)(p)

struct session session;

struct atomic_t{
    int value;
    pthread_spinlock_t atomic_lock
};

enum msg_opcode{
    DONE = 1,
    RESP_MR_MAP,
    RESP_MIG_MAP,
    REQ_QUERY,
    RESP_QUERY,
    EVICT,
    REQ_MR_MAP,
    REQ_MIG_MAP,
    REQ_MIGRATION,
    DONE_MIGRATION,
    DELETE_RB
};

enum remote_block_handler_state {
    RBH_IDLE,
    RBH_READY,
    RBH_EVICT,
    RBH_MIGRATION_READY,
    RBH_MIGRATION_DONE,
    RBH_DELETE_RB,
};

enum test_state {
    IDLE = 1,
    CONNECT_REQUEST,
    ADDR_RESOLVED,
    ROUTE_RESOLVED,
    CONNECTED,
    RECV_QUERY_REPL,
    RECV_MR_MAP,
    RECV_MIG_MAP,
    RDMA_READ_INP,
    RDMA_READ_COMPLETE,
    RDMA_WRITE_INP,
    RDMA_WRITE_COMPLETE,
    RDMA_SEND_COMPLETE,
    ERROR
};

enum conn_state {
    CONN_IDLE=0,
    CONN_CONNECTED,   //connected but not mapped 
    CONN_MAPPED,
    CONN_MIGRATING,
    CONN_EVICTING,
    CONN_FAIL
};

enum cb_state {
    CB_IDLE=0,
    CB_CONNECTED,   //connected but not mapped 
    CB_MAPPED,
    CB_MIGRATING,
    CB_EVICTING,
    CB_FAIL
};

struct mr_info {
    uint64_t buf; //for dma addr
    uint32_t rkey;
    uint32_t size;
};

struct message {
    enum msg_opcode type;
    int size_gb;
    int rb_index;
    int cb_index;
    int replica_index;
    uint64_t buf;
    uint32_t rkey;
    //char reserve[360]; // for inbox driver padding min 400byte ?
    char reserve[4056]; // for inbox driver padding min 4KB ?
};

struct portal {
    uint8_t addr[16];                    /* dst addr in NBO */
    uint16_t port;                  /* dst port in NBO */
};

enum remote_block_state {
    ACTIVE=1,
    READONLY,
    NONE_ALLOWED,
    INACTIVE
};

struct remote_block_header {
    struct timeval *tv;
    enum remote_block_state state;
    uint8_t addr[16];
    uint8_t reserve[4064];
};

struct remote_block {
    uint32_t remote_rkey;	
    uint64_t remote_addr;
    enum remote_block_state rb_state;
    int *bitmap_g;	
    int replica_index;
};

struct remote_block_handler {
    struct remote_block **remote_block;
    struct atomic_t *remote_mapped; //atomic_t type * MAX_RB_NUM

    int block_size_gb; //size = num block * ONE_GB
    int target_size_gb; // freemem from server
    unsigned long sum; // sum of age from server

    struct task_struct *mig_thread;
    int *rb_idx_map;	//block_index to swapspace index
    char *mig_rb_idx;
    sem_t sem;
    enum remote_block_handler_state c_state;

    int dest_rb_index;
    int dest_cb_index;
    int src_rb_index;
    int src_cb_index;
    int client_swapspace_index; 
    int replica_index;
};

struct rdma_mr {
    char *rdma_buf;
    struct ibv_mr *rdma_mr;
    uint32_t rkey;
};

struct rdma_ctx {
    int id;
    struct krping_cb *cb;
    struct remote_block *rb;
    unsigned long idx;
    int clientid;
    int opcode;
    struct rdma_mr *mr;
    struct rdma_mr dynamic_mr;
    unsigned long len;
    char* req_buf;

    struct ibv_send_wr rdma_sq_wr;	/* rdma work request record */
    struct ibv_sge rdma_sgl;		/* rdma single SGE */

    int rb_index;
    int replica_index;
    struct rdma_ctx *prime_ctx;
    int isDynamic;
    int numreq;
    struct urdmabox_req *reqlist[MAX_RDMA_WR_BATCH_LEN];
    struct atomic_t *ref_count;
    void* private_data; // private data for user
    struct local_page_list *sending_item;
};

typedef struct rdma_freepool_s {
    pthread_spinlock_t rdma_freepool_lock;
    unsigned long cap_nr_mr;          /* capacity. nr of elements at *elements */
    unsigned long curr_nr_mr;         /* Current nr of elements at *elements */
    void **mr_elements;
    int init_done;
}rdma_freepool_t;

struct session{
    struct portal listener_portal;
    struct portal *portal_list;
    struct ibv_pd *pd;
    int cb_num;
    struct krping_cb *listener_cb;
    struct krping_cb        **cb_list;
    enum cb_state *cb_state_list;
    struct task_struct *listener_cb_thread;

    rdma_freepool_t *mr_freepool;
    //rdma_freepool_t *ctx_freepool;
    lfqueue_t *ctx_freepool;
    pthread_spinlock_t cb_lock;

    struct atomic_t *cb_index_map[NUM_REPLICA];
    int *mapping_lbg_to_rb[NUM_REPLICA];

    lfqueue_t *wr_req_q;
    lfqueue_t *rd_req_q;

    pthread_t remote_sender_th;
    sem_t sending_list_sem;
};

struct krping_cb {
    int cb_index;
    struct session *sess;
    struct ibv_comp_channel *channel; //cq channel
    struct ibv_cq *cq;
    struct ibv_qp *qp;

    struct ibv_recv_wr rq_wr;	/* recv work request record */
    struct ibv_sge recv_sgl;		/* recv single SGE */
    struct message recv_buf;	/* malloc'd buffer */
    struct ibv_mr *recv_mr;

    struct ibv_send_wr sq_wr;	/* send work requrest record */
    struct ibv_sge send_sgl;
    struct message send_buf; /* single send buf */
    struct ibv_mr *send_mr;

    struct ibv_send_wr rdma_sq_wr;	/* rdma work request record */
    struct ibv_sge rdma_sgl;		/* rdma single SGE */
    char *rdma_buf;			/* used as rdma sink */
    struct ibv_mr *rdma_mr; // user buffer for test
    struct ibv_mr *rdma_mr_tmp; // user buffer for test

    struct remote_block_handler rbh;

    enum test_state state;		/* used for cond/signalling */
    sem_t sem;

    struct sockaddr_in sin;
    uint16_t port;			/* dst port in NBO */
    uint8_t addr[16];			/* dst addr in NBO */
    char ip6_ndev_name[128];	/* IPv6 netdev name */
    char *addr_str;			/* dst addr string */
    uint8_t addr_type;		/* ADDR_FAMILY - IPv4/V6 */

    pthread_t cqthread;

    /* CM stuff */
    struct rdma_event_channel *cm_channel;
    pthread_t cmthread;
    struct rdma_cm_id *cm_id;	/* connection on client side,*/
    struct rdma_cm_id *child_cm_id;	/* connection on server side */

};

#undef htonll
#undef ntohll
static inline uint64_t htonll(uint64_t x) { return htobe64(x); }
static inline uint64_t ntohll(uint64_t x) { return be64toh(x); }
#define htonll htonll
#define ntohll ntohll

struct callbacks {
    void (*write_done) (void*);
    void (*read_done) (void*);
};

static struct callbacks ops = {
    .write_done     = NULL,
    .read_done      = NULL,
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

//functions
int krping_cma_event_handler(struct rdma_cm_event *event);
void krping_free_qp(struct krping_cb *cb);
int krping_accept(struct krping_cb *cb, struct rdma_cm_id *cm_id);
void krping_free_buffers(struct krping_cb *cb);
int krping_setup_qp(struct krping_cb *cb, struct rdma_cm_id *cm_id);
int krping_setup_buffers(struct krping_cb *cb);
int setup_connection(struct session *sess, struct portal *portal, struct krping_cb *listener_cb);
int get_addr(char *dst, struct sockaddr_in *addr);
int krping_cq_event_handler(struct krping_cb *cb, struct ibv_wc *wc);
void connect_remote(struct krping_cb *cb, struct session *session);
void* client_thread_fn(void* data);
int control_block_init(struct krping_cb *cb, struct session *session);
void create_mr_pool(struct session *session);
int find_the_best_node(struct session *session, int *replica_list, int num_replica, int isMig);
int remote_block_map(struct session *session, int select_idx, struct krping_cb **replica_cb_list);
struct krping_cb* get_cb(struct session *session, int lbg_index, struct krping_cb **replica_cb_list);
//void get_ctx_freepool(rdma_freepool_t *pool, struct rdma_ctx **ctx_element);
struct rdma_ctx * get_ctx_freepool();
void get_mr_freepool(rdma_freepool_t *pool, struct rdma_mr **mr_element);
void atomic_init(struct atomic_t *m);
void atomic_set(struct atomic_t *m, int val);
int atomic_inc_return(struct atomic_t *m);
int atomic_dec(struct atomic_t *m);
int atomic_read(struct atomic_t *m);
int registercallback(char *func_str, void (*ptr)(void*));
int put_wr_req_item(void *tmp);
int get_wr_req_item(void **tmp);
int put_rd_req_item(void *tmp);
int get_rd_req_item(void **tmp);

#endif
