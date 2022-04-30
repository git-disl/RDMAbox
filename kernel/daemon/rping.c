/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
 *
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

#include "rping.h"
#include "debug.h"

struct rdma_session session;
long page_size;  

static struct context *s_ctx = NULL;

int get_addr(char *dst, struct sockaddr_in *addr)
{
	struct addrinfo *res;
	int ret;

	ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret) {
		printf("getaddrinfo failed - invalid hostname or IP address\n");
		return ret;
	}

	if (res->ai_family != PF_INET) {
		ret = -1;
		goto out;
	}

	*addr = *(struct sockaddr_in *) res->ai_addr;
out:
	freeaddrinfo(res);
	return ret;
}

void portal_parser(FILE *fptr)
{
  char line[256];
  int p_count=0, i=0, j=0;
  int port;

  fgets(line, sizeof(line), fptr);
  sscanf(line, "%d", &p_count);
  session.mig_server_num = p_count;
  printf("daemon[%s]: num_servers : %d \n",__func__,session.mig_server_num);

  session.portal_list = malloc(sizeof(struct portal) * p_count);

  while (fgets(line, sizeof(line), fptr)) {
      printf("%s", line);
      j = 0;
      while (*(line + j) != ':'){
          j++;
      }
      memcpy(session.portal_list[i].addr, line, j);
      session.portal_list[i].addr[j] = '\0';
      port = 0;
      sscanf(line+j+1, "%d", &port);
      session.portal_list[i].port = (uint16_t)port;
      printf("daemon[%s]: portal: %s, %d\n",__func__, session.portal_list[i].addr, session.portal_list[i].port);
      i++;
  }
}

void read_portal_file(char *filepath)
{
   FILE *fptr;

   printf("reading portal file.. [%s]\n",filepath);
   if ((fptr = fopen(filepath,"r")) == NULL){
       printf("portal file open failed.");
       exit(1);
   }

   portal_parser(fptr);

   fclose(fptr);
}

uint64_t htonll(uint64_t value)
{
     int num = 42;
     if(*(char *)&num == 42)
          return ((uint64_t)htonl(value & 0xFFFFFFFF) << 32LL) | htonl(value >> 32);
     else
          return value;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void post_send(struct rping_cb *cb)
{
      int ret=0;
      struct ibv_send_wr *bad_wr;

      cb->sq_wr.wr_id = (uintptr_t)cb;

      ret = ibv_post_send(cb->qp[0], &cb->sq_wr, &bad_wr);
      if (ret) {
		fprintf(stderr, "post send error %d\n", ret);
		exit(-1);
	}
}

void post_receive(struct rping_cb *cb, struct ibv_qp *qp)
{
      int ret=0;
      struct ibv_recv_wr *bad_wr;

      cb->rq_wr.wr_id = (uintptr_t)cb;

      ret = ibv_post_recv(qp, &cb->rq_wr, &bad_wr);
	if (ret) {
		fprintf(stderr, "ibv_post_recv failed: %d\n", ret);
		exit(-1);
	}
}

void post_read(struct rping_cb *cb, uint32_t rkey, uint64_t addr, int region_index)
{
  int ret;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  //printf("rpg %s : rkey:%"PRIu32 ", addr:%"PRIu64", chunk_header:%zu \n", __func__,rkey, addr,sizeof(struct chunk_header));

  wr.wr_id = (uintptr_t)cb;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = (uintptr_t)addr;
  wr.wr.rdma.rkey = rkey;

  sge.addr = (uintptr_t)session.rdma_remote.region_list[region_index];
  sge.length = ONE_GB;
  sge.lkey = session.rdma_remote.mr_list[region_index]->lkey;

  ret = ibv_post_send(cb->qp[0], &wr, &bad_wr);
  if (ret) {
	fprintf(stderr, "post send error %d\n", ret);
  }
}

void post_write(struct rping_cb *cb, uint32_t rkey, uint64_t addr, int region_index)
{
  int ret;
  struct ibv_send_wr wr[2], *bad_wr = NULL;
  struct ibv_sge sge_fst;
  struct ibv_sge sge_snd;

  memset(&wr[0], 0, sizeof(wr[0]));
  memset(&wr[1], 0, sizeof(wr[1]));

  printf("rpg %s : rkey:%"PRIu32 ", addr:%"PRIu64" \n", __func__,rkey, addr);

  sge_fst.addr = (uintptr_t)session.rdma_remote.region_list[region_index];
  sge_fst.length = HALF_ONE_GB;
  sge_fst.lkey = session.rdma_remote.mr_list[region_index]->lkey;

  sge_snd.addr = (uintptr_t)(session.rdma_remote.region_list[region_index] + HALF_ONE_GB);
  sge_snd.length = HALF_ONE_GB;
  sge_snd.lkey = session.rdma_remote.mr_list[region_index]->lkey;

  wr[0].opcode = IBV_WR_RDMA_WRITE;
  wr[0].sg_list = &sge_fst;
  wr[0].num_sge = 1;
  wr[0].send_flags = 0;
  wr[0].wr.rdma.remote_addr = (uintptr_t)addr;
  wr[0].wr.rdma.rkey = rkey;
  wr[0].next = &wr[1];

  wr[1].wr_id = (uintptr_t)cb;
  wr[1].opcode = IBV_WR_RDMA_WRITE;
  wr[1].sg_list = &sge_snd;
  wr[1].num_sge = 1;
  wr[1].send_flags = IBV_SEND_SIGNALED;
  wr[1].wr.rdma.remote_addr = (uintptr_t)(addr + HALF_ONE_GB);
  wr[1].wr.rdma.rkey = rkey;
  wr[1].next = NULL;

  ret = ibv_post_send(cb->qp[0], &wr[0], &bad_wr);
  if (ret) {
	fprintf(stderr, "post send error %d\n", ret);
  }
}


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

void rdma_session_init(struct rdma_session *sess, struct rping_cb *servercb){
  int free_mem_g;
  int i;
  page_size = getpagesize();

  free_mem_g = (int)(get_free_mem() / ONE_MB);
  printf("rpg %s :  get free_mem %d\n", __func__, free_mem_g);
  for (i=0; i<MAX_FREE_MEM_GB; i++) {
    sess->rdma_remote.conn_map[i] = -1;
    sess->rdma_remote.conn_chunk_map[i] = -1;
    sess->rdma_remote.malloc_map[i] = CHUNK_EMPTY;
  }

  if (free_mem_g > FREE_MEM_EXPAND_THRESHOLD){
    free_mem_g -= (FREE_MEM_EVICT_THRESHOLD + FREE_MEM_EXPAND_THRESHOLD) / 2;
  } else if (free_mem_g > FREE_MEM_EVICT_THRESHOLD){
    free_mem_g  -= FREE_MEM_EVICT_THRESHOLD;
  }else{
    free_mem_g = 0;
  }
  if (free_mem_g > MAX_FREE_MEM_GB) {
    free_mem_g = MAX_FREE_MEM_GB;
  }

  for (i=0; i < free_mem_g; i++){
    posix_memalign((void **)&(sess->rdma_remote.region_list[i]), page_size, ONE_GB);
    memset(sess->rdma_remote.region_list[i], 0x00, ONE_GB);
    sess->rdma_remote.malloc_map[i] = CHUNK_MALLOCED;
  }
  sess->rdma_remote.size_gb = free_mem_g;
  sess->rdma_remote.mapped_size = 0;

  for (i=0; i<MAX_CLIENT; i++){
    sess->cb_list[i] = NULL;
    sess->cb_state_list[i] = CB_IDLE;
  }
  sess->conn_num = 0;

  // mig cb list init
  sess->mig_cb_list = (struct rping_cb **)malloc(sizeof(struct rping_cb *) * sess->mig_server_num);
  sess->mig_cb_state_list = (enum cb_state *)malloc(sizeof(enum cb_state) * sess->mig_server_num);

  for (i=0; i<session.mig_server_num; i++) {
    printf("rpg %s : init mig cb addr:%s port:%d \n", __func__, sess->portal_list[i].addr, sess->portal_list[i].port);
    sess->mig_cb_state_list[i] = CB_IDLE;
    sess->mig_cb_list[i] = malloc(sizeof(struct rping_cb));
    memset(sess->mig_cb_list[i], 0, sizeof(struct rping_cb));
    sess->mig_cb_list[i]->state = IDLE;
    sess->mig_cb_list[i]->port = htons(sess->portal_list[i].port);
    inet_pton(AF_INET6, sess->portal_list[i].addr, &sess->mig_cb_list[i]->addr);
    sess->mig_cb_list[i]->cb_index = i;
    sess->mig_cb_list[i]->addr_type = AF_INET6;
    sem_init(&sess->mig_cb_list[i]->sem, 0, 0);

    get_addr(sess->portal_list[i].addr, &sess->mig_cb_list[i]->sin);
    sess->mig_cb_list[i]->sin.sin_port = htons(sess->portal_list[i].port);

    if(servercb->sin.sin_addr.s_addr == sess->mig_cb_list[i]->sin.sin_addr.s_addr && servercb->port == sess->mig_cb_list[i]->sin.sin_port){
        printf("rpg %s : This node matches to index[%d] mig_cb addr:%s port:%d \n", __func__,i, sess->portal_list[i].addr, sess->portal_list[i].port);
        sess->my_index=i;
    }

  }//for

  printf("rpg %s :  allocated mem %d\n", __func__, sess->rdma_remote.size_gb);

}

void mig_setup_connection(struct rdma_cm_id *cm_id, struct rping_cb *listening_cb, int mig_cb_index)
{
	struct ibv_recv_wr *bad_wr;
	int ret;
        int i;

	struct rping_cb *cb = malloc(sizeof(struct rping_cb));
	if (!cb){
	    DEBUG_LOG("fail to allocate cb \n");
	    return;
        }

        cb->server = 1;
	cb->cm_id[0] = cm_id;
        cb->cm_id[0]->context = cb;
        cb->cm_id_index = 0;
        cb->qp_index = 0;

        ret = rping_server_setup_cq(cm_id,listening_cb);
	if (ret) {
		fprintf(stderr, "setup_qp failed: %d\n", ret);
		goto err0;
	}

	ret = rping_create_qp(cb,cm_id,0);
	if (ret) {
		fprintf(stderr, "rping_create_qp failed: %d\n", ret);
		goto err3;
	}
	DEBUG_LOG("created qp %p\n", cb->qp);

        cb->sess = &session;
        for (i = 0; i < MAX_FREE_MEM_GB; i++){
            cb->sess_chunk_map[i] = -1;
        }
        cb->mapped_chunk_size = 0;

        sem_init(&cb->evict_sem, 0, 0);
        sem_init(&cb->mig_chunk.migration_sem, 0, 0);

        DEBUG_LOG("setup mig_cb[%d] \n",mig_cb_index);
        //add to session 
        if (session.mig_cb_state_list[mig_cb_index] == CB_IDLE){
	        DEBUG_LOG("add mig_cb to the list : %d \n",mig_cb_index);
                session.mig_cb_list[mig_cb_index] = cb;
                session.mig_cb_state_list[mig_cb_index] = CB_CONNECTED;
                cb->cb_index = mig_cb_index;
        }else{
                // not going to happen
	        DEBUG_LOG("already addded to the list \n");
        }
        session.mig_conn_num += 1;

	ret = rping_setup_buffers(cb);
	if (ret) {
		fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
		goto err1;
	}

        post_receive(cb, cb->qp[0]);

	ret = rping_accept(cb, cm_id);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err3;
	}

	return ;
err3:
	pthread_cancel(s_ctx->cq_poller_thread);
	pthread_join(s_ctx->cq_poller_thread, NULL);
err2:
	rping_free_buffers(cb);
err1:
	rping_free_qp(cb,0);
err0:
	free_cb(cb);
	return ;
}

int rping_cma_event_handler(struct rdma_cm_event *event)
{
	int ret = 0;
        struct conn_private_data *pdata;
	int mig_cb_index;
	struct rdma_cm_id *cma_id = event->id;
	struct rping_cb *cb = cma_id->context; // listening_cb

	printf("cma_event type %s cma_id %p\n",
		  rdma_event_str(event->event), cma_id);

	switch (event->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
		cb->state = ADDR_RESOLVED;
		ret = rdma_resolve_route(cma_id, 2000);
		if (ret) {
			cb->state = ERROR;
			fprintf(stderr, "rdma_resolve_route error %d\n", ret);
			sem_post(&cb->sem);
		}
		break;

	case RDMA_CM_EVENT_ROUTE_RESOLVED:
		cb->state = ROUTE_RESOLVED;
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_CONNECT_REQUEST:
		cb->state = CONNECT_REQUEST;
		cb->child_cm_id = cma_id;
                pdata = (struct conn_private_data *)event->param.conn.private_data;
                mig_cb_index = pdata->mig_cb_index;
                //printf("daemon[%s]: RDMA_CM_EVENT_CONNECT_REQUEST cma_id:%p cm_id:%p child_cm_id:%p src_region_index:%d\n",__func__,cma_id, cb->cm_id, cb->child_cm_id,src_region_index);
                //printf("daemon[%s]: RDMA_CM_EVENT_CONNECT_REQUEST cma_id:%p cm_id:%p child_cm_id:%p\n",__func__,cma_id, cb->cm_id, cb->child_cm_id);
                //printf("daemon[%s]: cm_id:%p child_cm_id:%p \n",__func__, cma_id, cb->child_cm_id);
                printf("daemon[%s]: incoming src addr : %s , mig_cb_index : %d\n",__func__, pdata->addr, pdata->mig_cb_index);

                if(mig_cb_index != -1){
                    mig_setup_connection(cma_id,cb,mig_cb_index); //incomming cm_id
                }else{
                    setup_connection(cma_id,cb, pdata); //incomming cm_id
                }
		break;

	case RDMA_CM_EVENT_ESTABLISHED:
                //printf("receive RDMA_CM_EVENT_ESTABLISHED\n");
    	        cb->state = CONNECTED;
                printf("daemon[%s]: RDMA_CM_EVENT_ESTABLISHED cma_id:%p cm_id:%p child_cm_id:%p cb:%p\n",__func__,cma_id, cb->cm_id, cb->child_cm_id,cb);
                if(cb->server){
                    send_free_mem_size(cb);
                    //post_receive(cb, cb->qp[0]);
		}
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_ADDR_ERROR:
                printf("receive RDMA_CM_EVENT_ADDR_ERROR \n");
		sem_post(&cb->sem);
		break;
	case RDMA_CM_EVENT_ROUTE_ERROR:
                printf("receive RDMA_CM_EVENT_ROUTE_ERROR \n");
		sem_post(&cb->sem);
		break;
	case RDMA_CM_EVENT_CONNECT_ERROR:
                printf("receive RDMA_CM_EVENT_CONNECT_ERROR \n");
		sem_post(&cb->sem);
		break;
	case RDMA_CM_EVENT_UNREACHABLE:
                printf("receive RDMA_CM_EVENT_UNREACHABLE \n");
		sem_post(&cb->sem);
		break;
	case RDMA_CM_EVENT_REJECTED:
                printf("receive RDMA_CM_EVENT_REJECTED \n");
		sem_post(&cb->sem);
		ret = -1;
		break;

	case RDMA_CM_EVENT_DISCONNECTED:
		fprintf(stderr, "DISCONNECT EVENT...\n");
		sem_post(&cb->sem);
		break;

	case RDMA_CM_EVENT_DEVICE_REMOVAL:
		fprintf(stderr, "cma detected device removal!!!!\n");
		ret = -1;
		break;

	default:
		fprintf(stderr, "unhandled event: %s, ignoring\n",
			rdma_event_str(event->event));
		break;
	}

	return ret;
}

void send_free_mem_size(void *context)
{
  struct rping_cb *cb = (struct rping_cb *)context;
  struct timeval tmp_tv;
  unsigned long sum=0;
  int free_mem=0;
  int i;
  int grad=0;

  cb->qmsg.type = RESP_QUERY;
  free_mem = session.rdma_remote.size_gb - session.rdma_remote.mapped_size;
  if(free_mem < 0)
      cb->qmsg.size_gb = 0;
  else
      cb->qmsg.size_gb = free_mem;

  // option 1 : share avg grad in window
  for (i = 0; i < GRAD_WINDOW; i++){
      grad += session.grad[i];
      //printf("grad[%d]=%d\n",i,session.grad[i]);
  }
  cb->qmsg.grad = grad/GRAD_WINDOW;

  /*
  // option 2 : share current grad
  free_mem_g = (int)(get_free_mem() / ONE_MB);
  cb->qmsg.grad = free_mem_g - session.free_mem_prev;
  */

  printf("RDMAbox[%s] : send free mem : %d, grad : %d,  last_mem : %d\n", __func__, cb->qmsg.size_gb, cb->qmsg.grad, session.free_mem_prev);
  memcpy(cb->send_buf ,&cb->qmsg,sizeof(struct query_message));
  post_send(cb);
}

void send_evict(void *context, int evict_index, int client_chunk_index, int src_cb_index)
{
  int i;
  struct rping_cb *cb = (struct rping_cb *)context;

  cb->send_buf->type = EVICT;
  cb->send_buf->size_gb = client_chunk_index;
  cb->send_buf->cb_index = src_cb_index;
  cb->send_buf->chunk_index = evict_index;

  for (i=0; i<MAX_MR_SIZE_GB; i++){
    if(i == evict_index){
        cb->send_buf->rkey[i] = htonl((uint64_t)session.rdma_remote.mr_list[i]->rkey);
        cb->send_buf->buf[i] = htonll((uint64_t)session.rdma_remote.mr_list[i]->addr);
        cb->send_buf->replica_index = session.rdma_remote.replica_index[i];
    }else{
        cb->send_buf->rkey[i] = 0;
    }
  }

  printf("rpg %s : send_evict chunk[%d] rkey:%"PRIu32", addr:%"PRIu64", replica_index:%d \n", __func__,evict_index,cb->send_buf->rkey[evict_index],cb->send_buf->buf[evict_index],cb->send_buf->replica_index);
  post_send(cb);
}

int prepare_single_mr(void *context, int isMigration)
{
  struct rping_cb *cb = (struct rping_cb *)context;
  int i = 0;
  int client_chunk_index = cb->recv_buf->size_gb;

  printf("rpg %s : swapspace[%d], replica_list[%d] \n",__func__,client_chunk_index,cb->recv_buf->replica_index);
  for (i=0; i<MAX_FREE_MEM_GB;i++){
    cb->send_buf->rkey[i] = 0;
  }
  for (i=0; i<MAX_FREE_MEM_GB; i++) {
    if (session.rdma_remote.malloc_map[i] == CHUNK_MALLOCED && session.rdma_remote.conn_map[i] == -1) {// allocated && unmapped 
      cb->sess_chunk_map[i] = i;
      session.rdma_remote.replica_index[i] = cb->recv_buf->replica_index;
      session.rdma_remote.conn_map[i] = cb->cb_index; // this cb_index is for local 
      session.rdma_remote.client_chunk_index[i] = client_chunk_index;
      session.rdma_remote.mr_list[i] = ibv_reg_mr(s_ctx->pd, session.rdma_remote.region_list[i], ONE_GB, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ); //Write permission can't cover read permission, different traditional understanding
      cb->send_buf->buf[i] = htonll((uint64_t)session.rdma_remote.mr_list[i]->addr);
      cb->send_buf->rkey[i] = htonl((uint64_t)session.rdma_remote.mr_list[i]->rkey);
      printf("rpg %s : client_chunk[%d] is mapped to (chunk[%d] thisnode[%d]). RDMA addr %llx  rkey %x\n",__func__,client_chunk_index,i,cb->cb_index,(unsigned long long)cb->send_buf->buf[i], cb->send_buf->rkey[i]);
      break;
    }
  }

  session.rdma_remote.mapped_size += 1;
  cb->mapped_chunk_size += 1;

  cb->send_buf->size_gb = client_chunk_index;
  cb->send_buf->replica_index = cb->recv_buf->replica_index;
  if(isMigration){
      cb->send_buf->type = INFO_MIGRATION;
      cb->send_buf->cb_index = session.my_index; // this is global my ID
      cb->send_buf->chunk_index = i;
  }else{
      cb->send_buf->type = INFO_SINGLE;
      //cb->send_buf->cb_index = cb->recv_buf->cb_index;
  }

  //post_send(cb);
  
  return i;
}

void* do_migration_fn(void *data)
{
     struct rping_cb *cb = data;
     struct rping_cb *mig_cb;

     while (1) {
         printf("rpg [%s]: migration handler go to sleep and wait...\n", __func__);
         sem_wait(&cb->mig_chunk.migration_sem);

         request_migration(cb->mig_chunk.local_chunk_index,cb->mig_chunk.src_chunk_index,cb->mig_chunk.src_cb_index);

     }//while
}

void request_migration(int local_chunk_index, int src_chunk_index, int src_cb_index)
{
  int i;
  struct rping_cb *mig_cb;
  uint64_t addr;
  uint32_t rkey;

  mig_cb =  session.mig_cb_list[src_cb_index];
  printf("rpg %s : src_chunk[%d] on src_node[%d]\n", __func__,src_chunk_index,src_cb_index);

  // check if connected
  if(mig_cb->state < CONNECTED){
      printf("rpg %s : Not connected yet. Try to connect to src_node[%d]\n", __func__,src_cb_index);
      rping_create_connection(mig_cb);
      printf("rpg %s : Connection to src_node[%d] is done.\n", __func__,src_cb_index);
  }

  // send migration request message to src node
  mig_cb->send_buf->size_gb = src_chunk_index;
  mig_cb->send_buf->type = REQ_MIGRATION;
  for (i=0; i<MAX_MR_SIZE_GB; i++){
    if(i==local_chunk_index){
        mig_cb->send_buf->rkey[i] = htonl((uint64_t)session.rdma_remote.mr_list[i]->rkey);
        mig_cb->send_buf->buf[i] = htonll((uint64_t)session.rdma_remote.mr_list[i]->addr);
    }else{
        mig_cb->send_buf->rkey[i] = 0;
    }
  }
  printf("rpg %s : send migration request to src_chunk[%d] on src_node[%d]\n", __func__,src_chunk_index,src_cb_index);
  post_send(mig_cb);

}


void do_migration(struct rping_cb *cb)
{
  int i;
  struct rping_cb *mig_cb;
  int dest_cb_index;
  uint64_t addr;
  uint32_t rkey;

  cb->mig_chunk.local_chunk_index = cb->recv_buf->size_gb;
  dest_cb_index = cb->recv_buf->cb_index;

  for (i = 0 ; i<MAX_MR_SIZE_GB; i++) {
    if (cb->recv_buf->rkey[i]){
	 rkey = ntohl(cb->recv_buf->rkey[i]);
	 addr = ntohll(cb->recv_buf->buf[i]);
	 break;
    }
  }
  cb->state = RDMA_WRITE_INP;
  printf("rpg %s : Migration start. src_chunk[%d] -> dest_chunk[%d] dest_node[%d]\n", __func__,cb->mig_chunk.local_chunk_index,i,dest_cb_index);
  post_write(cb, rkey, addr, cb->mig_chunk.local_chunk_index);
}

void recv_delete_chunk(struct rping_cb *cb)
{
  int i, index;

  for (i = 0 ; i<MAX_MR_SIZE_GB; i++) {
    if (cb->recv_buf->rkey[i]){ //stopped this one
      index = cb->sess_chunk_map[i];
      cb->sess_chunk_map[i] = -1;
      ibv_dereg_mr(session.rdma_remote.mr_list[index]);
      free(session.rdma_remote.region_list[index]);
      session.rdma_remote.conn_map[index] = -1;
      session.rdma_remote.malloc_map[index] = CHUNK_EMPTY;
      session.rdma_remote.conn_chunk_map[index] = -1;
      break;
    }
  }

  session.rdma_remote.size_gb -= 1;
  session.rdma_remote.mapped_size -= 1;
  cb->mapped_chunk_size -= 1;

  printf("rpg %s :  Delete Chunk is done chunk[%d] \n", __func__,i);
  sem_post(&cb->evict_sem);
}

int handle_recv(struct rping_cb *cb)
{
     int i;
     struct query_message qmsg;

     switch (cb->recv_buf->type){
      case QUERY:
        printf("rpg %s :  QUERY \n", __func__);
        send_free_mem_size(cb);
        break;
      case BIND_MIGRATION:
        // send mr info back to the client 
        printf("rpg %s : receive BIND_MIGRATION src_chunk_index:%d \n", __func__,cb->recv_buf->chunk_index);
        cb->state = BIND_MIG_RECV;

        session.cb_state_list[cb->cb_index] = CB_MAPPED;

        cb->mig_chunk.client_chunk_index = cb->recv_buf->size_gb;
        cb->mig_chunk.src_chunk_index = cb->recv_buf->chunk_index;
        cb->mig_chunk.src_cb_index = cb->recv_buf->cb_index;
        for (i = 0 ; i<MAX_MR_SIZE_GB; i++) {
          if (cb->recv_buf->rkey[i]){
	    cb->mig_chunk.rkey[i] = ntohl(cb->recv_buf->rkey[i]);
	    cb->mig_chunk.buf[i] = ntohll(cb->recv_buf->buf[i]);
            printf("rpg %s : rkey:%"PRIu32 ", addr:%"PRIu64", src_chunk_index:%d\n", __func__,cb->mig_chunk.rkey[i], cb->mig_chunk.buf[i],i);
	    break;
          }
        }
        cb->mig_chunk.local_chunk_index = prepare_single_mr(cb, 1);
        post_send(cb);
	// send completion will reelease migration_sem

        // read chunk from src node to the new chunk on this node
        //request_migration(cb,local_chunk_index,src_chunk_index,src_cb_index);
        break;
      case BIND_SINGLE:
        printf("rpg %s :  BIND_SINGLE \n", __func__);
        //size_gb => client swap space index
        prepare_single_mr(cb, 0);
        post_send(cb);
        session.cb_state_list[cb->cb_index] = CB_MAPPED;
        break;
      case DELETE_CHUNK:
        printf("rpg %s :  DELETE_CHUNK \n", __func__);
        recv_delete_chunk(cb);
        break;
      // daemon to daemon
      case RESP_QUERY:
	memcpy(&qmsg,&cb->recv_buf,sizeof(qmsg));
        printf("[%d]rpg %s : RESP_QUERY free_mem : %d, sum : %lu \n",getpid(), __func__,qmsg.size_gb, qmsg.sum);
	cb->state = FREE_MEM_RECV;
	//sem_post(&cb->sem);
        break;
      case REQ_MIGRATION:
        printf("[%d]rpg %s : REQ_MIGRATION local_chunk[%d] \n",getpid(), __func__,cb->recv_buf->size_gb);
	cb->state = REQ_MIG_RECV;
        do_migration(cb); //post receive inside
        break;
/*
      case RESP_ACTIVITY:
        printf("[%d]rpg %s : RESP_ACTIVITY local_chunk[%d] \n",getpid(), __func__,cb->recv_buf->size_gb);
        sem_post(&cb->evict_sem);
        break;
*/
      default:
        printf("unknown received message : %d\n",cb->recv_buf->type);
        return -1;
    }

	return 0;
}

/* 
//experiments for existing approaches
#define EXTRA_CHUNK_NUM 2
void evict_mem_random(int evict_g)
{
  int i,j,n;
  int oldest_chunk_index=-1;
  int freed_g = 0;
  int client_cb_index;
  struct rping_cb *client_cb;
  int send_list[MAX_FREE_MEM_GB];
  long candidate_activity[MAX_FREE_MEM_GB];
  long tmp_ul;
  int final_choice=0;
  long final_activity;
  int random_num;

  printf("rpg %s : need to evict %d GB\n",__func__, evict_g);
  //free unmapped chunk
  for (i = 0; i < MAX_FREE_MEM_GB ;i++) {
    if (session.rdma_remote.malloc_map[i] == CHUNK_MALLOCED && session.rdma_remote.conn_map[i] == -1){
      free(session.rdma_remote.region_list[i]);
      session.rdma_remote.malloc_map[i] = CHUNK_EMPTY;
      freed_g += 1;
      if (freed_g == evict_g){
        session.rdma_remote.size_gb -= evict_g;
        printf("rpg %s : free unmapped chunk %d\n",__func__, freed_g);
        return;
      }
    }
  }
  //not enough
  session.rdma_remote.size_gb -= freed_g;
  evict_g -= freed_g;

  evict_g += EXTRA_CHUNK_NUM;
  if (session.rdma_remote.mapped_size < evict_g){
      evict_g = session.rdma_remote.mapped_size;
  }

  while(evict_g > 0){

      for (i=0; i<MAX_FREE_MEM_GB; i++){
          send_list[i] = 0;
      }

      for (j = 0; j < (evict_g); j++){
          random_num = rand() % MAX_FREE_MEM_GB;

retry:
          while (session.rdma_remote.conn_map[random_num] == -1){
            random_num = rand() % MAX_FREE_MEM_GB;
	  }

          for (i=0; i<MAX_FREE_MEM_GB; i++){
                if (send_list[i] == 1 && i == random_num){
                    random_num = rand() % MAX_FREE_MEM_GB;
		    goto retry;
		}
	   }

          printf("rpg %s : choose random chunk[%d]\n",__func__,random_num);
          send_list[random_num] = 1;
	  final_choice = random_num;
      }

      n=0;
      client_cb_index = session.rdma_remote.conn_map[final_choice];
      client_cb = session.cb_list[client_cb_index];

      for (i=0; i<MAX_FREE_MEM_GB; i++){
        if (send_list[i] == 1){
            client_cb->send_buf->rkey[i] = htonl(session.rdma_remote.client_chunk_index[i]);
	    n++;
        }else{
            client_cb->send_buf->rkey[i] = 0;
	}
      }
      printf("rpg %s : send REQ_ACTIVITY message..\n",__func__);
      send_query(client_cb);

      printf("rpg %s : wait for RESP_ACTIVITY message.. \n",__func__);
      sem_wait(&client_cb->evict_sem);

      for (i=0; i<MAX_FREE_MEM_GB; i++){
        if (client_cb->recv_buf->rkey[i] == 1){
	    candidate_activity[i] = ntohll(client_cb->recv_buf->buf[i]);
            printf("rpg %s : Received activity chunk[%d] activity=%lu\n",__func__,i,candidate_activity[i]);
	    final_choice = candidate_activity[i];
	    final_choice = i;
        }else{
	    candidate_activity[i] = -1;
        }
      }

     for (i=0; i<MAX_FREE_MEM_GB; i++) {
       if ((final_choice > candidate_activity[i]) && candidate_activity[i] != -1) {
            final_activity = candidate_activity[i];               
	    final_choice = i;
       }
     }

     printf("rpg %s : final_choice chunk[%d]=%lu\n",__func__,final_choice,final_activity);

     printf("rpg %s : send_evict selected chunk[%d] client_index[%d]\n", __func__,final_choice, session.rdma_remote.client_chunk_index[final_choice]);
     send_evict(client_cb, final_choice, session.rdma_remote.client_chunk_index[final_choice], session.my_index );

  printf("rpg %s : wait for DELETE_CHUNK message..\n",__func__);
  sem_wait(&client_cb->evict_sem);
  printf("rpg %s : Migration Done. \n",__func__);

  --evict_g;
  }//while
}
*/

void evict_mem(int evict_g)
{
  int i;
  int oldest_chunk_index=-1;
  int freed_g = 0;
  int client_cb_index;
  struct rping_cb *client_cb;
  struct timeval oldest;
  struct timeval tmp_tv;

  printf("rpg %s : need to evict %d GB\n",__func__, evict_g);
  //free unmapped chunk
  for (i = 0; i < MAX_FREE_MEM_GB ;i++) {
    if (session.rdma_remote.malloc_map[i] == CHUNK_MALLOCED && session.rdma_remote.conn_map[i] == -1){
      free(session.rdma_remote.region_list[i]);
      session.rdma_remote.malloc_map[i] = CHUNK_EMPTY;
      freed_g += 1;
      if (freed_g == evict_g){
        session.rdma_remote.size_gb -= evict_g;
        printf("rpg %s : free unmapped chunk %d\n",__func__, freed_g);
        return;
      }
    }
  }
  //not enough
  session.rdma_remote.size_gb -= freed_g;
  evict_g -= freed_g;

  if (session.rdma_remote.mapped_size < evict_g){
      evict_g = session.rdma_remote.mapped_size;
  }
  while(evict_g > 0){
  // sort by age
  // pick the oldest
  // is this globaly synced time ?
  oldest.tv_sec=0;
  oldest.tv_usec=0;
  for (i = 0; i < MAX_FREE_MEM_GB; i++){
      if (session.rdma_remote.conn_map[i] != -1){
            memcpy(&tmp_tv, session.rdma_remote.region_list[i], sizeof(struct timeval));
            if(oldest.tv_sec==0 && oldest.tv_usec==0){
               oldest = tmp_tv;
               oldest_chunk_index = i;
            }else{
               if(oldest.tv_sec*1000000L + oldest.tv_usec > tmp_tv.tv_sec*1000000L + tmp_tv.tv_usec){
                   oldest = tmp_tv;
                   oldest_chunk_index = i;
               }
            }
            printf("TimeStamp : chunk[%d] : %ld , oldest : %ld \n",i,tmp_tv.tv_sec*1000000L + tmp_tv.tv_usec,oldest.tv_sec*1000000L + oldest.tv_usec);
      }
  }

  // send evict message of the oldest chunk to client
  client_cb_index = session.rdma_remote.conn_map[oldest_chunk_index];
  client_cb = session.cb_list[client_cb_index];
  printf("rpg %s : send_evict selected chunk[%d] client_index[%d]\n", __func__,oldest_chunk_index, session.rdma_remote.client_chunk_index[oldest_chunk_index]);
  send_evict(client_cb, oldest_chunk_index, session.rdma_remote.client_chunk_index[oldest_chunk_index], session.my_index );

  printf("rpg %s : wait for DELETE_CHUNK message..\n",__func__);
  sem_wait(&client_cb->evict_sem);
  printf("rpg %s : Migration Done. \n",__func__);
  --evict_g;
  }//while
}

int rping_cq_event_handler(struct rping_cb *cb, struct ibv_wc *wc)
{
	int ret = 0;
        int client_cb_index;
        struct rping_cb *client_cb;

	if (wc->status) {
		fprintf(stderr, "cq completion failed status %s\n",
		ibv_wc_status_str(wc->status));
		if (wc->status != IBV_WC_WR_FLUSH_ERR)
			ret = -1;
		goto error;
	}

	switch (wc->opcode) {
	case IBV_WC_SEND:
		DEBUG_LOG("send completion\n");
       		if(cb->state == BIND_MIG_RECV){
		    DEBUG_LOG("BIND_MIG info sent done\n");
       		    sem_post(&cb->mig_chunk.migration_sem);
		}
		cb->state = RDMA_SEND_COMPLETE;
		break;
	case IBV_WC_RDMA_WRITE:
		DEBUG_LOG("rdma write completion\n");

		// send migration done message to client
  		printf("rpg %s : send Migration Done message. \n", __func__);
  		client_cb_index = session.rdma_remote.conn_map[cb->mig_chunk.local_chunk_index];
  		client_cb = session.cb_list[client_cb_index];
  		client_cb->send_buf->type = DONE_MIGRATION;
  		post_send(client_cb);

		cb->state = RDMA_WRITE_COMPLETE;
		break;
	case IBV_WC_RDMA_READ:
		DEBUG_LOG("rdma read completion\n");
		cb->state = RDMA_READ_COMPLETE;
  		sem_post(&cb->mig_chunk.migration_sem);
		break;
	case IBV_WC_RECV:
		DEBUG_LOG("recv completion\n");
		handle_recv(cb);
		post_receive(cb, cb->qp[0]);
		break;
	default:
		DEBUG_LOG("unknown!!!!! completion\n");
		ret = -1;
		goto error;
	}

	return 0;

error:
	cb->state = ERROR;
	return ret;
}

int rping_accept(struct rping_cb *cb, struct rdma_cm_id *cm_id)
{
	struct rdma_conn_param conn_param;
	int ret;

	DEBUG_LOG("accepting client connection request\n");

	memset(&conn_param, 0, sizeof conn_param);
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
        conn_param.rnr_retry_count = 7; /* infinite retry */

	ret = rdma_accept(cm_id, &conn_param);
	if (ret) {
		fprintf(stderr, "rdma_accept error: %d\n", ret);
		return ret;
	}

	return 0;
}

void rping_setup_wr(struct rping_cb *cb)
{
	cb->recv_sgl.addr = (uintptr_t) cb->recv_buf;
	cb->recv_sgl.length = sizeof(struct message);
	cb->recv_sgl.lkey = cb->recv_mr->lkey;
	cb->rq_wr.sg_list = &cb->recv_sgl;
	cb->rq_wr.num_sge = 1;

	cb->send_sgl.addr = (uintptr_t) cb->send_buf;
	cb->send_sgl.length = sizeof(struct message);
	cb->send_sgl.lkey = cb->send_mr->lkey;
	cb->sq_wr.opcode = IBV_WR_SEND;
	cb->sq_wr.send_flags = IBV_SEND_SIGNALED;
	cb->sq_wr.sg_list = &cb->send_sgl;
	cb->sq_wr.num_sge = 1;
}

int rping_setup_buffers(struct rping_cb *cb)
{
	int ret;

        cb->send_buf = malloc(sizeof(struct message));
        cb->recv_buf = malloc(sizeof(struct message));

	//TODO : test code
	    DEBUG_LOG("rping_setup_buffers called on cb %p\n", cb);
	    cb->recv_mr = ibv_reg_mr(s_ctx->pd, cb->recv_buf, sizeof(struct message),
				 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
	    if (!cb->recv_mr) {
		fprintf(stderr, "recv_buf reg_mr failed\n");
		return errno;
	    }

	    cb->send_mr = ibv_reg_mr(s_ctx->pd, cb->send_buf, sizeof(struct message),
                                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
	    if (!cb->send_mr) {
		fprintf(stderr, "send_buf reg_mr failed\n");
		ret = errno;
		goto err1;
	    }

	rping_setup_wr(cb);
	DEBUG_LOG("allocated & registered buffers...\n");
	return 0;

err1:
	ibv_dereg_mr(cb->recv_mr);
	return ret;
}

void rping_free_buffers(struct rping_cb *cb)
{
	DEBUG_LOG("rping_free_buffers called on cb %p\n", cb);
	ibv_dereg_mr(cb->recv_mr);
	ibv_dereg_mr(cb->send_mr);
}

int rping_create_qp(struct rping_cb *cb, struct rdma_cm_id *cm_id, int qp_idx)
{
	struct ibv_qp_init_attr init_attr;
	int ret=0;

	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.cap.max_send_wr = 16351;
	init_attr.cap.max_recv_wr = 10240;
	init_attr.cap.max_recv_sge = 1;
	init_attr.cap.max_send_sge = 1;
	init_attr.qp_type = IBV_QPT_RC;

	if (cb->server) {
	        init_attr.send_cq = s_ctx->cq;
	        init_attr.recv_cq = s_ctx->cq;
		ret = rdma_create_qp(cm_id, s_ctx->pd, &init_attr);
	} else {
	        init_attr.send_cq = cb->cq;
		init_attr.recv_cq = cb->cq;
		ret = rdma_create_qp(cm_id, s_ctx->pd, &init_attr);
	}

	if (!ret){
                DEBUG_LOG("created qp[%d] \n",qp_idx);
		cb->qp[qp_idx] = cm_id->qp;
	}else{
                printf("fail to create qp[%d] %d\n",qp_idx,ret);
        }

	return ret;
}

void rping_free_qp(struct rping_cb *cb, int qp_idx)
{
        ibv_destroy_qp(cb->qp[qp_idx]);
        if(cb->server){
	    ibv_destroy_cq(s_ctx->cq);
	    ibv_destroy_comp_channel(s_ctx->comp_channel);
	    ibv_dealloc_pd(s_ctx->pd);
        }else{
	    ibv_destroy_cq(cb->cq);
	    ibv_destroy_comp_channel(cb->channel);
	    ibv_dealloc_pd(cb->pd);
        }
}

void * receiver_cq_thread(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;
  int curr;
  int ret;
  int ne;

  while (1) {

#ifdef USE_HYBRID_POLL
event:
        ret = ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx);
        if (ret) {
		fprintf(stderr, "Failed to get cq event!\n");
		pthread_exit(NULL);
        }

        ret = ibv_req_notify_cq(cq, 0);
        if (ret) {
		fprintf(stderr, "Failed to set notify!\n");
		pthread_exit(NULL);
        }
        curr = 0;

	do {
             curr++; 
             ne = ibv_poll_cq(cq, 1, &wc);
        } while (ne== 0 && curr < TIMEOUT);

        ret = rping_cq_event_handler((struct rping_cb *)(uintptr_t)wc.wr_id, &wc);
	ibv_ack_cq_events(cq, 1);
	if (ret)
	    pthread_exit(NULL);

        if(curr >= TIMEOUT){
             if(ne == 0){
                  goto event;
             }
             curr = 0;
        }
#endif
#ifdef USE_EVENT_POLL
        if(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx) != 0){
            printf("ibv_get_cq_event fail\n");
            exit(-1);
        }
        ibv_ack_cq_events(cq, 1);
        if(ibv_req_notify_cq(cq, 0) != 0 ){
            printf("ibv_req_notify_cq fail\n");
            exit(-1);
        }

        while (ibv_poll_cq(cq, 1, &wc))
            rping_cq_event_handler((struct rping_cb *)(uintptr_t)wc.wr_id, &wc);
#endif
#ifdef USE_BUSY_POLL
	do {
             ne = ibv_poll_cq(s_ctx->cq, 1, &wc);
        } while (ne== 0);

        ret = rping_cq_event_handler((struct rping_cb *)(uintptr_t)wc.wr_id, &wc);
	ibv_ack_cq_events(s_ctx->cq, 1);
	if (ret)
	    pthread_exit(NULL);
#endif

   }//while

  return NULL;
}

void *sender_cq_thread(void *arg)
{
	struct rping_cb *cb = arg;
	struct ibv_cq *ev_cq;
	void *ev_ctx;
	int ret;
        struct ibv_wc wc;
        int curr;
        int ne;
	
	DEBUG_LOG("cq_thread started.\n");

	while (1) {	
		pthread_testcancel();
#ifdef USE_HYBRID_POLL
event:
	        ret = ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx);
     	        if (ret) {
			fprintf(stderr, "Failed to get cq event!\n");
			pthread_exit(NULL);
	        }
	        if (ev_cq != cb->cq) {
			fprintf(stderr, "Unknown CQ!\n");
			pthread_exit(NULL);
	        }
	        ret = ibv_req_notify_cq(cb->cq, 0);
	        if (ret) {
			fprintf(stderr, "Failed to set notify!\n");
			pthread_exit(NULL);
	        }
                curr = 0;

		do {
                    curr++; 
                    ne = ibv_poll_cq(cb->cq, 1, &wc);
                } while (ne== 0 && curr < TIMEOUT);

		ret = rping_cq_event_handler(cb, &wc);
		ibv_ack_cq_events(cb->cq, 1);
		if (ret)
			pthread_exit(NULL);

                if(curr >= TIMEOUT){
                   if(ne == 0){
                       goto event;
                   }
                   curr = 0;
                }
#endif
#ifdef USE_EVENT_POLL
                ret = ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx);
                if (ret) {
                        fprintf(stderr, "Failed to get cq event!\n");
                        pthread_exit(NULL);
                }
                if (ev_cq != cb->cq) {
                        fprintf(stderr, "Unknown CQ!\n");
                        pthread_exit(NULL);
                }
                ret = ibv_req_notify_cq(cb->cq, 0);
                if (ret) {
                        fprintf(stderr, "Failed to set notify!\n");
                        pthread_exit(NULL);
                }

	        while ((ret = ibv_poll_cq(cb->cq, 1, &wc)) == 1) {
                    ret = rping_cq_event_handler(cb, &wc);
                }
                    ibv_ack_cq_events(cb->cq, 1);
                    if (ret)
                        pthread_exit(NULL);
#endif
#ifdef USE_BUSY_POLL
		do {
                    ne = ibv_poll_cq(cb->cq, 1, &wc);
                } while (ne== 0);

		ret = rping_cq_event_handler(cb, &wc);
		ibv_ack_cq_events(cb->cq, 1);
		if (ret)
			pthread_exit(NULL);
#endif


	}//while
}

int rping_setup_cq(struct rping_cb *cb, struct rdma_cm_id *cm_id)
{
	int ret;

  	cb->pd = s_ctx->pd;

	cb->channel = ibv_create_comp_channel(cm_id->verbs);
	if (!cb->channel) {
		fprintf(stderr, "ibv_create_comp_channel failed\n");
		ret = errno;
		goto err1;
	}
	DEBUG_LOG("created channel %p\n", cb->channel);

	cb->cq = ibv_create_cq(cm_id->verbs,QUEUE_DEPTH * submit_queues * num_server , cb, cb->channel, 0);
	if (!cb->cq) {
		fprintf(stderr, "ibv_create_cq failed\n");
		ret = errno;
		goto err2;
	}
	DEBUG_LOG("created cq %p\n", cb->cq);

	ret = ibv_req_notify_cq(cb->cq, 0);
	if (ret) {
		fprintf(stderr, "ibv_req_notify_cq failed\n");
		ret = errno;
		goto err3;
	}

	return 0;

err3:
	ibv_destroy_cq(cb->cq);
err2:
	ibv_destroy_comp_channel(cb->channel);
err1:
	ibv_dealloc_pd(cb->pd);
	return ret;
}

int rping_server_setup_cq(struct rdma_cm_id *cm_id, struct rping_cb *listening_cb)
{
	int ret;

        if (s_ctx) {
            printf("s_ctx exist\n");

            if (s_ctx->ctx != cm_id->verbs)
                die("cannot handle events in more than one context.\n");

            goto initdone;
        }

        s_ctx = (struct context *)malloc(sizeof(struct context));

        s_ctx->ctx = cm_id->verbs;

  	s_ctx->pd = ibv_alloc_pd(s_ctx->ctx);
	if (!s_ctx->pd) {
		fprintf(stderr, "ibv_alloc_pd failed\n");
		return errno;
	}
	DEBUG_LOG("created pd %p\n", s_ctx->pd);

	s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx);
	if (!s_ctx->comp_channel) {
		fprintf(stderr, "ibv_create_comp_channel failed\n");
		ret = errno;
		goto err1;
	}
	DEBUG_LOG("created channel %p\n", s_ctx->comp_channel);

	s_ctx->cq = ibv_create_cq(s_ctx->ctx, 100, listening_cb, s_ctx->comp_channel, 0);
	if (!s_ctx->cq) {
		fprintf(stderr, "ibv_create_cq failed\n");
		ret = errno;
		goto err2;
	}
	DEBUG_LOG("created cq %p\n", s_ctx->cq);

	ret = ibv_req_notify_cq(s_ctx->cq, 0);
	if (ret) {
		fprintf(stderr, "ibv_req_notify_cq failed\n");
		ret = errno;
		goto err3;
	}

        pthread_create(&s_ctx->cq_poller_thread, NULL, receiver_cq_thread, NULL);

initdone:

	return 0;

err3:
	ibv_destroy_cq(s_ctx->cq);
err2:
	ibv_destroy_comp_channel(s_ctx->comp_channel);
err1:
	ibv_dealloc_pd(s_ctx->pd);

	return ret;
}

void *cm_thread(void *arg)
{
	struct rping_cb *cb = arg;
	struct rdma_cm_event *event;
	int ret;

	while (1) {
		ret = rdma_get_cm_event(cb->cm_channel, &event);
		if (ret) {
			fprintf(stderr, "rdma_get_cm_event err %d\n", ret);
			exit(ret);
		}
                struct rdma_cm_event event_copy;

   		memcpy(&event_copy, event, sizeof(*event));
		rdma_ack_cm_event(event);

		ret = rping_cma_event_handler(&event_copy);
		if (ret)
			exit(ret);
	}
}

void fill_sockaddr(struct rping_cb *cb, struct portal *addr_info)
{
  if (cb->addr_type == AF_INET) {
    cb->port = htons(addr_info->port);
    inet_pton(AF_INET, addr_info->addr, &cb->addr);

    get_addr(addr_info->addr, &cb->sin);
    cb->sin.sin_port = htons(addr_info->port);

  } else if (cb->addr_type == AF_INET6) {
    cb->port = htons(addr_info->port);
    inet_pton(AF_INET6, addr_info->addr, &cb->addr);

    get_addr(addr_info->addr, &cb->sin);
    cb->sin.sin_port = htons(addr_info->port);
  }
}

int rping_bind_server(struct rping_cb *cb)
{
	int ret;
        uint16_t port = 0;
        char buffer[20];

        cb->sin.sin_port = cb->port;
      	ret = rdma_bind_addr(cb->cm_id[0],(struct sockaddr *)&cb->sin);
	if (ret) {
		fprintf(stderr, "rdma_bind_addr error %d\n", ret);
		return ret;
	}

	ret = rdma_listen(cb->cm_id[0], 3);
	if (ret) {
		fprintf(stderr, "rdma_listen failed: %d\n", ret);
		return ret;
	}
        port = ntohs(rdma_get_src_port(cb->cm_id[0]));

        inet_ntop(AF_INET6, rdma_get_local_addr(cb->cm_id[0]), buffer, 20);
        printf("daemon %s : listening on port %s %d.\n",__func__,buffer, port);

	return 0;
}

void free_cb(struct rping_cb *cb)
{
	free(cb);
}

void setup_connection(struct rdma_cm_id *cm_id, struct rping_cb *listening_cb, struct conn_private_data *pdata)
{
	struct ibv_recv_wr *bad_wr;
	int ret;
        int i;
        int cmid_idx;
        int qp_idx;
        int cb_exist=0;
        struct rping_cb *cb;

        //check if this cm_id is already registered.
        printf("daemon[%s]: incoming setup request cb addr:%s \n",__func__, pdata->addr);

        for (i=0; i<MAX_CLIENT; i++){
            if (session.cb_state_list[i] == CB_CONNECTED){
                if(strcmp(pdata->addr, session.cb_list[i]->addr) == 0){
                    printf("daemon[%s]: cb exists addr:%s \n",__func__, pdata->addr);
		    cb_exist = 1;
		    cb = session.cb_list[i];
                    break;
                }
            }
        }

        if(!cb_exist){
	    cb = malloc(sizeof(struct rping_cb));
	    if (!cb){
	        DEBUG_LOG("fail to allocate cb \n");
	        return;
            }
            cb->cm_id_index = 0;
            cb->qp_index = 0;
            cb->server = 1;
            printf("daemon[%s]: cb doesn't exists. create new cb. \n",__func__);

            //add to session 
            for (i=0; i<MAX_CLIENT; i++){
                if (session.cb_state_list[i] == CB_IDLE){
                    session.cb_list[i] = cb;
                    session.cb_state_list[i] = CB_CONNECTED;
                    cb->cb_index = i;
                    memcpy(session.cb_list[i]->addr, pdata->addr, sizeof(session.cb_list[i]->addr));
                    printf("daemon[%s]: register cb_list[%d] addr:%s pdata:%s size:%lu\n",__func__,i,session.cb_list[i]->addr, pdata->addr, sizeof(session.cb_list[i]->addr));
                    break;
                }
            }

        }

        cmid_idx = cb->cm_id_index++;
        qp_idx = cb->qp_index++;

        DEBUG_LOG("cmid_idx[%d] qp_idx[%d] \n",cmid_idx,qp_idx);

	cb->cm_id[cmid_idx] = cm_id;
        cb->cm_id[cmid_idx]->context = cb;

        ret = rping_server_setup_cq(cm_id,listening_cb);
	if (ret) {
	    fprintf(stderr, "setup_cq failed: %d\n", ret);
    	    goto err0;
        }

	ret = rping_create_qp(cb,cm_id,qp_idx);
	if (ret) {
		fprintf(stderr, "rping_create_qp failed: %d\n", ret);
		goto err3;
	}

        if(!cb->sess){
            cb->sess = &session;
            for (i = 0; i < MAX_FREE_MEM_GB; i++){
                cb->sess_chunk_map[i] = -1;
            }
            cb->mapped_chunk_size = 0;

            sem_init(&cb->evict_sem, 0, 0);
            sem_init(&cb->mig_chunk.migration_sem, 0, 0);
            DEBUG_LOG("setup client_cb from client \n");

            pthread_create(&cb->mig_chunk.migthread, NULL, do_migration_fn, cb);

            session.conn_num += 1;

	    ret = rping_setup_buffers(cb);
	    if (ret) {
		fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
		goto err1;
	    }
        }

        post_receive(cb,cb->qp[qp_idx]);

	ret = rping_accept(cb,cm_id);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err3;
	}

	return ;
err3:
	pthread_cancel(s_ctx->cq_poller_thread);
	pthread_join(s_ctx->cq_poller_thread, NULL);
err2:
	rping_free_buffers(cb);
err1:
	rping_free_qp(cb,qp_idx);
err0:
	free_cb(cb);
	return ;
}

int start_persistent_listener(struct rping_cb *listening_cb)
{
        int ret;
	struct rping_cb *cb;
	struct rdma_cm_event *event;

        listening_cb->server = 1;

	listening_cb->cm_channel = rdma_create_event_channel();
	if (!listening_cb->cm_channel) {
		ret = errno;
		fprintf(stderr, "rdma_create_event_channel error %d\n", ret);
		return ret;
	}

	ret = rdma_create_id(listening_cb->cm_channel, &listening_cb->cm_id[0], listening_cb, RDMA_PS_TCP);
	if (ret) {
		ret = errno;
		fprintf(stderr, "rdma_create_id error %d\n", ret);
		return ret;
	}
	DEBUG_LOG("created cm_id %p\n", listening_cb->cm_id[0]);

	ret = rping_bind_server(listening_cb);
	if (ret)
		return ret;

 
         while (1) {
                 ret = rdma_get_cm_event(listening_cb->cm_channel, &event);
                 if (ret) {
                         fprintf(stderr, "rdma_get_cm_event err %d\n", ret);
                         exit(ret);
                 }
                 struct rdma_cm_event event_copy;
 
                 memcpy(&event_copy, event, sizeof(*event));
                 rdma_ack_cm_event(event);
 
                 ret = rping_cma_event_handler(&event_copy);
                 if (ret)
                         exit(ret);
        }

        DEBUG_LOG("destroy cm_id %p\n", listening_cb->cm_id[0]);
        rdma_destroy_id(listening_cb->cm_id[0]);
        rdma_destroy_event_channel(listening_cb->cm_channel);
        free(listening_cb);

    return 0;
}

int rping_connect_client(struct rping_cb *cb, struct rdma_cm_id *cm_id)
{
	struct rdma_conn_param conn_param;
        struct conn_private_data pdata;
	int ret;
        pdata.mig_cb_index = session.my_index;

	memset(&conn_param, 0, sizeof conn_param);
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 10;
        conn_param.private_data = &pdata;
        conn_param.private_data_len = sizeof(struct conn_private_data);

	ret = rdma_connect(cm_id, &conn_param);
	if (ret) {
		fprintf(stderr, "rdma_connect error %d\n", ret);
		return ret;
	}

	sem_wait(&cb->sem);
	if (cb->state != CONNECTED) {
		fprintf(stderr, "wait for CONNECTED state %d\n", cb->state);
		return -1;
	}

	DEBUG_LOG("rmda_connect successful\n");
	return 0;
}

int rping_bind_client(struct rping_cb *cb, struct rdma_cm_id *cm_id)
{
	int ret;

	ret = rdma_resolve_addr(cm_id, NULL, (struct sockaddr *) &cb->sin, 2000);
	if (ret) {
		fprintf(stderr, "rdma_resolve_addr error %d\n", ret);
		return ret;
	}

	sem_wait(&cb->sem);
	if (cb->state != ROUTE_RESOLVED) {
		fprintf(stderr, "waiting for addr/route resolution state %d\n",
			cb->state);
		return -1;
	}

	DEBUG_LOG("rdma_resolve_addr - rdma_resolve_route successful\n");
	return 0;
}

int rping_create_connection(struct rping_cb *cb)
{
	struct ibv_recv_wr *bad_wr;
	int ret;

	cb->cm_channel = rdma_create_event_channel();
	if (!cb->cm_channel) {
		ret = errno;
		fprintf(stderr, "rdma_create_event_channel error %d\n", ret);
		return ret;
	}

	ret = rdma_create_id(cb->cm_channel, &cb->cm_id[0], cb, RDMA_PS_TCP);
	if (ret) {
		ret = errno;
		fprintf(stderr, "rdma_create_id error %d\n", ret);
		return ret;
	}
	DEBUG_LOG("created cm_id %p\n", cb->cm_id[0]);

	pthread_create(&cb->cmthread, NULL, cm_thread, cb);

	ret = rping_bind_client(cb, cb->cm_id[0]);
	if (ret)
		return ret;

	ret = rping_setup_cq(cb, cb->cm_id[0]);
	if (ret) {
		fprintf(stderr, "setup_cq failed: %d\n", ret);
		return ret;
	}

	ret = rping_create_qp(cb, cb->cm_id[0], 0);
	if (ret) {
		fprintf(stderr, "rping_create_qp failed: %d\n", ret);
		return ret;
	}
	DEBUG_LOG("created qp %p\n", cb->qp[0]);

	ret = rping_setup_buffers(cb);
	if (ret) {
		fprintf(stderr, "rping_setup_buffers failed: %d\n", ret);
		goto err1;
	}

        post_receive(cb, cb->qp[0]);      
	pthread_create(&cb->cqthread, NULL, sender_cq_thread, cb);

	ret = rping_connect_client(cb, cb->cm_id[0]);
	if (ret) {
		fprintf(stderr, "connect error %d\n", ret);
		goto err2;
	}
 
        return ret;

err2:
	rping_free_buffers(cb);
err1:
	rping_free_qp(cb,0);

	return ret;
}

void* free_mem_monitor_fn(void *data)
{
  int free_mem_g = 0;
  int last_free_mem_g;
  int filtered_free_mem_g = 0;
  int evict_hit_count = 0;
  int expand_hit_count = 0;
  float last_free_mem_weight = 1 - CURR_FREE_MEM_WEIGHT;
  int stop_size_g;
  int expand_size_g;
  int i, j;
  struct timeval tv;
  struct rping_cb *mig_cb;
  int running=1;
  int cur_grad_pos=0;
  int grad_avg=0; //for debug


  last_free_mem_g = (int)(get_free_mem() / ONE_MB);
  for(i=0;i<GRAD_WINDOW;i++){
      session.grad[i] = 0;
  }
  session.free_mem_prev = free_mem_g;
  printf("rpg %s :  is called, last %d GB, weight: %f, %f\n", __func__, last_free_mem_g, CURR_FREE_MEM_WEIGHT, last_free_mem_weight);

  while (running) {// server is working
     /* //option 2 : periodically update free_mem_prev
     if(grad_period%GRAD_PERIOD == 0){
         session.free_mem_prev = free_mem_g;
         grad_period=0;
     }
     grad_period++;
     */

     // option 1 : share avg grad in window. keep updating the grad window
     session.free_mem_prev = free_mem_g;
     free_mem_g = (int)(get_free_mem() / ONE_MB);

     session.grad[(cur_grad_pos%GRAD_WINDOW)] = free_mem_g - session.free_mem_prev;

     for (i = 0; i < GRAD_WINDOW; i++){
       grad_avg += session.grad[i];
     }
     printf("free:%d free_prev:%d grad[%d]:%d grad_avg:%d\n",free_mem_g, session.free_mem_prev, cur_grad_pos%GRAD_WINDOW, session.grad[cur_grad_pos%GRAD_WINDOW],grad_avg/GRAD_WINDOW);
     grad_avg=0;

     cur_grad_pos++;

    //need a filter
    filtered_free_mem_g = (int)(CURR_FREE_MEM_WEIGHT * free_mem_g + last_free_mem_g * last_free_mem_weight);
    last_free_mem_g = filtered_free_mem_g;

    for (i = 0; i < MAX_FREE_MEM_GB; i++){
      if (session.rdma_remote.conn_map[i] != -1){
            memcpy(&tv, session.rdma_remote.region_list[i], sizeof(struct timeval));
            printf("TimeStamp : chunk[%d] %ld microseconds\n",i,tv.tv_sec*1000000L + tv.tv_usec);
      }
    }
    printf("TimeStamp : \n");

   int input;
   printf("Enter GB to evict \n");
   scanf("%d", &input);
   while ((getchar()) != '\n'); 
   printf("entered number : %d \n", input);
   evict_mem(input);
   //evict_mem_random(input);
   continue;

    if (filtered_free_mem_g < FREE_MEM_EVICT_THRESHOLD){
      evict_hit_count += 1;
      expand_hit_count = 0;
      if (evict_hit_count >= MEM_EVICT_HIT_THRESHOLD){
        evict_hit_count = 0;
        //evict  down_threshold - free_mem
        stop_size_g = FREE_MEM_EVICT_THRESHOLD - last_free_mem_g;
        printf("rpg %s : evict %d GB ",__func__, stop_size_g);
        if (session.rdma_remote.size_gb < stop_size_g){
          stop_size_g = session.rdma_remote.size_gb;
        }
        if (stop_size_g > 0){ //stop_size_g has to be meaningful.
          evict_mem(stop_size_g);
          //evict_mem_random(stop_size_g);
        }
        last_free_mem_g += stop_size_g;
      }
    }else if (filtered_free_mem_g > FREE_MEM_EXPAND_THRESHOLD) {
      expand_hit_count += 1;
      evict_hit_count = 0;
      if (expand_hit_count >= MEM_EXPAND_HIT_THRESHOLD){
        expand_hit_count = 0;
        expand_size_g =  last_free_mem_g - FREE_MEM_EXPAND_THRESHOLD;
        if ((expand_size_g + session.rdma_remote.size_gb) > MAX_FREE_MEM_GB) {
          expand_size_g = MAX_FREE_MEM_GB - session.rdma_remote.size_gb;
        }
        j = 0;
        for (i = 0; i < MAX_FREE_MEM_GB; i++){
          if (session.rdma_remote.malloc_map[i] == CHUNK_EMPTY){
            posix_memalign((void **)&(session.rdma_remote.region_list[i]), page_size, ONE_GB);
            memset(session.rdma_remote.region_list[i], 0x00, ONE_GB);
            session.rdma_remote.malloc_map[i] = CHUNK_MALLOCED;
            j += 1;
            if (j == expand_size_g){
              break;
            }
          }
        }
        session.rdma_remote.size_gb += expand_size_g;
        last_free_mem_g -= expand_size_g;
      }
    }

    // printf("\n"); 
    sleep(1);
  }
  return NULL;
}

void usage(char *name)
{
	printf("%s ip_addr port portal_file\n",name);
        exit(1);
}

int main(int argc, char *argv[])
{
	struct rping_cb *cb;
	int ret = 0;

        pthread_t client_session_thread;
        pthread_t free_mem_monitor_thread;
  
	page_size = sysconf(_SC_PAGE_SIZE);
 	
	cb = malloc(sizeof(*cb));

	DEBUG_LOG("create listening_cb %p\n", cb);

	if (!cb)
		return -ENOMEM;

	memset(cb, 0, sizeof(*cb));
	cb->state = IDLE;
	sem_init(&cb->sem, 0, 0);
        cb->cm_id_index = 0;
        cb->qp_index = 0;

        int op;
	int opterr = 0;
	while ((op=getopt(argc, argv, "a:p:i:")) != -1) {
		switch (op) {
		case 'a':
			ret = get_addr(optarg, &cb->sin);
			break;
		case 'p':
			cb->port = htons(atoi(optarg));
			DEBUG_LOG("port %d\n", (int) atoi(optarg));
			break;
		case 'i':
                        read_portal_file(optarg);
			break;
		default:
			ret = EINVAL;
			goto out;
		}
	}
	if (ret)
		goto out;

        // pre-allocate memory region
        rdma_session_init(&session, cb);

        pthread_create(&free_mem_monitor_thread, NULL, free_mem_monitor_fn, NULL);

        start_persistent_listener(cb); //block here

	DEBUG_LOG("destroy cm_id %p\n", cb->cm_id[0]);
	rdma_destroy_id(cb->cm_id[0]);
out2:
	rdma_destroy_event_channel(cb->cm_channel);
out:
	free(cb);
	return ret;
}
