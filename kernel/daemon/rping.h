/*
 * Copyright (c) 2005 Ammasso, Inc. All rights reserved.
 * Copyright (c) 2006 Open Grid Computing, Inc. All rights reserved.
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

#ifndef RPING_H
#define RPING_H

/*
#include <sys/types.h>
#include <sys/socket.h>
#include <byteswap.h>
#include <infiniband/arch.h>
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
//#include <stdatomic.h>

//#define GRAD_PERIOD 8
#define GRAD_WINDOW 8

//#define USE_HYBRID_POLL
//#define TIMEOUT 1000
#define USE_EVENT_POLL
//#define USE_BUSY_POLL

#define MAX_CLIENT      32

#define NUM_CHANNEL 4

//for c320
//#define MAX_FREE_MEM_GB 8 //for local memory management
//#define MAX_MR_SIZE_GB 8 //for msg passing

#define MAX_FREE_MEM_GB 48 //for local memory management
#define MAX_MR_SIZE_GB 48 //for msg passing

#define ONE_MB 1048576
#define ONE_GB 1073741824
#define HALF_ONE_GB 536870912
//for c320
//#define FREE_MEM_EVICT_THRESHOLD 4 //in GB
//#define FREE_MEM_EXPAND_THRESHOLD 8 // in GB
#define FREE_MEM_EVICT_THRESHOLD 10 //in GB
#define FREE_MEM_EXPAND_THRESHOLD 20 // in GB

#define CURR_FREE_MEM_WEIGHT 0.7
#define MEM_EVICT_HIT_THRESHOLD 1
#define MEM_EXPAND_HIT_THRESHOLD 20

#define CHUNK_MALLOCED 1
#define CHUNK_EMPTY     0

#define QUEUE_DEPTH 256
#define submit_queues 32
#define num_server 3
/*
//bitmap
#define BITMAP_SHIFT 6 // 2^5=32
#define BITMAP_MASK 0x1f // 2^5=32
#define BITMAP_INT_SIZE 1 //bitmap[], 1GB/4k/32
*/
#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

#define ntohll(x) (((uint64_t)(ntohl((int)((x << 32) >> 32))) << 32) | \
        (unsigned int)ntohl(((int)(x >> 32))))

int debug = 1;
#define DEBUG_LOG if (debug) printf

/*
 * These states are used to signal events between the completion handler
 * and the main client or server thread.
 *
 * Once CONNECTED, they cycle through RDMA_READ_ADV, RDMA_WRITE_ADV, 
 * and RDMA_WRITE_COMPLETE for each ping.
 */

struct portal {
        uint8_t addr[16];                    /* dst addr in NBO */
        uint16_t port;                  /* dst port in NBO */
};

struct shm_message {
       struct rdma_cm_id *cm_id;       /* connection on client side,*/
       struct rdma_cm_event *event;
};

enum test_state {
	IDLE = 1,
	CONNECT_REQUEST,
	ADDR_RESOLVED,
	ROUTE_RESOLVED,
	CONNECTED,
        FREE_MEM_RECV,
	AFTER_FREE_MEM,
	BIND_MIG_RECV,
	REQ_MIG_RECV,
	RDMA_READ_INP,
	RDMA_READ_COMPLETE,
	RDMA_WRITE_INP,
	RDMA_WRITE_COMPLETE,
	RDMA_SEND_COMPLETE,
	ERROR
};

enum cb_state{
  CB_IDLE,
  CB_CONNECTED,
  CB_MAPPED,
  CB_FAILED
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

struct message {
  enum msg_opcode type;
  int size_gb;
  int chunk_index;
  int cb_index;
  int replica_index;
  uint64_t buf[MAX_MR_SIZE_GB];
  uint32_t rkey[MAX_MR_SIZE_GB];
  char reserve[3496]; // for inbox driver padding min 4KB ?
};

struct query_message{
  enum msg_opcode type;
  int size_gb;
  int grad;
  unsigned long sum;
  char reserve[4080]; // for inbox driver padding min 4KB ?
};

struct conn_private_data{
  int mig_cb_index;
  uint8_t addr[16]; 
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
        uint8_t addr[16];
        uint8_t reserve[4064];
};
*/
#define RPING_SQ_DEPTH 16

/* Default string for print data and
 * minimum buffer size
 */
#define _stringify( _x ) # _x
#define stringify( _x ) _stringify( _x )

#define RPING_MSG_FMT           "rdma-ping-%d: "
#define RPING_MIN_BUFSIZE       sizeof(stringify(INT_MAX)) + sizeof(RPING_MSG_FMT)

enum chunk_list_state {
         C_IDLE,
         C_MIGRATION_READY,
         C_MIGRATION_DONE,
         C_DELETE_CHUNK
};

struct mig_chunk {
  uint64_t buf[MAX_MR_SIZE_GB];
  uint32_t rkey[MAX_MR_SIZE_GB];
  int client_chunk_index;
  int client_cb_index;
  int src_chunk_index;
  int src_cb_index;
  int local_chunk_index;
  pthread_t migthread;
  sem_t migration_sem;
  enum chunk_list_state state;
};
/*
struct mig_chunk_list {
        struct mig_chunk **chunk;
	pthread_t migthread;
        sem_t migration_sem;
        char *mig_chunk_map;
        enum chunk_list_state state;
};
*/
/*
 * Control block struct.
 */
struct rping_cb {
        int cm_id_index;
        int qp_index;
        //atomic_int qp_index;
        //atomic_int cm_id_index;
        //atomic_int qp_index;
	int server;			/* 0 iff client */
        int cb_index;
	pthread_t cqthread;
	pthread_t persistent_server_thread;
	struct ibv_comp_channel *channel;
	struct ibv_cq *cq;
	struct ibv_pd *pd;
	struct ibv_qp *qp[NUM_CHANNEL];

	struct ibv_recv_wr rq_wr;	/* recv work request record */
	struct ibv_sge recv_sgl;	/* recv single SGE */
	struct message *recv_buf;/* malloc'd buffer */
	struct ibv_mr *recv_mr;		/* MR associated with this buffer */

	struct ibv_send_wr sq_wr;	/* send work request record */
	struct ibv_sge send_sgl;
	struct query_message qmsg;/* single send buf */
	struct message *send_buf;/* single send buf */
	struct ibv_mr *send_mr;

	//struct ibv_send_wr rdma_sq_wr;	/* rdma work request record */
	//struct ibv_sge rdma_sgl;	/* rdma single SGE */
	//char *rdma_buf;			/* used as rdma sink */
	//struct ibv_mr *rdma_mr;

	uint32_t remote_rkey;		/* remote guys RKEY */
	uint64_t remote_addr;		/* remote guys TO */
	uint32_t remote_len;		/* remote guys LEN */

	enum test_state state;		/* used for cond/signalling */
	sem_t evict_sem;
	sem_t sem;

        struct sockaddr_in sin;
        uint8_t addr_type;              /* ADDR_FAMILY - IPv4/V6 */
        uint8_t addr[16];                    /* dst addr in NBO */
        uint16_t port;                  /* dst port in NBO */
	int verbose;			/* verbose logging */
 
	//int *bitmap_server_conn;  // client's server map

	/* CM stuff */
	pthread_t cmthread;
	struct rdma_event_channel *cm_channel;
	struct rdma_cm_id *cm_id[NUM_CHANNEL];	/* connection on client side,*/
					/* listener on service side. */
	struct rdma_cm_id *child_cm_id;	/* connection on server side */

        struct rdma_session *sess;
        int sess_chunk_map[MAX_MR_SIZE_GB];
        int mapped_chunk_size;
        
        //struct mig_chunk_list mig_chunk_list;
        struct mig_chunk mig_chunk;
};

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct rdma_remote_mem{
  char* region_list[MAX_FREE_MEM_GB];
  struct ibv_mr* mr_list[MAX_FREE_MEM_GB];
  int size_gb;
  int mapped_size;
  int conn_map[MAX_FREE_MEM_GB]; //chunk is used by which connection, or -1
  int client_chunk_index[MAX_FREE_MEM_GB]; //chunk is used by which connection, or -1
  int malloc_map[MAX_FREE_MEM_GB];
  int conn_chunk_map[MAX_FREE_MEM_GB]; //session_chunk 
  int replica_index[MAX_FREE_MEM_GB];
};

struct rdma_session {
  int my_index;
  struct rping_cb *cb_list[MAX_CLIENT]; // need to init NULL
  enum cb_state cb_state_list[MAX_CLIENT];
  int conn_num;

  struct rping_cb **mig_cb_list; // need to init NULL
  enum cb_state *mig_cb_state_list;
  int mig_conn_num;
  int mig_server_num;

  int free_mem_prev;
  int grad[GRAD_WINDOW];

  struct rdma_remote_mem rdma_remote;
  struct chunk_activity *evict_list;

  struct portal *portal_list;
};

void send_free_mem_size(void *context);
void setup_connection(struct rdma_cm_id *cm_id, struct rping_cb *listening_cb, struct conn_private_data *pdata);
void request_migration(int local_chunk_index, int src_chunk_index, int src_cb_index);
//void do_migration(struct rping_cb *cb);
int rping_create_connection(struct rping_cb *cb);
void fill_sockaddr(struct rping_cb *cb, struct portal *addr_info);
void* do_migration_fn(void *data);

int get_addr(char *dst, struct sockaddr_in *addr);
void portal_parser(FILE *fptr);
void read_portal_file(char *filepath);
int rping_cma_event_handler(struct rdma_cm_event *event);
int handle_recv(struct rping_cb *cb);
int rping_cq_event_handler(struct rping_cb *cb, struct ibv_wc *wc);
int rping_accept(struct rping_cb *cb, struct rdma_cm_id *cm_id);
void rping_setup_wr(struct rping_cb *cb);
int rping_setup_buffers(struct rping_cb *cb);
void rping_free_buffers(struct rping_cb *cb);
int rping_create_qp(struct rping_cb *cb, struct rdma_cm_id *cm_id, int qp_idx);
void rping_free_qp(struct rping_cb *cb, int qp_idx);
void * receiver_cq_thread(void *ctx);
void *sender_cq_thread(void *arg);
int rping_setup_cq(struct rping_cb *cb, struct rdma_cm_id *cm_id);
int rping_server_setup_cq(struct rdma_cm_id *cm_id, struct rping_cb *listening_cb);
void *cm_thread(void *arg);
int rping_bind_server(struct rping_cb *cb);
int start_persistent_listener(struct rping_cb *listening_cb);
int rping_connect_client(struct rping_cb *cb, struct rdma_cm_id *cm_id);
int rping_bind_client(struct rping_cb *cb, struct rdma_cm_id *cm_id);
void usage(char *name);
void free_cb(struct rping_cb *cb);

#endif
