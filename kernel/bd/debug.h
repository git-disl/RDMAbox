#define DEBUG_LINE printk("[%s][%s +%d] \n",__func__,__FILE__,__LINE__);

#ifdef DEBUG_RING
#define PRINT_RING(m) printk("rpg[%s] %s \n",__func__,#m);
#else
#define PRINT_RING(m)
#endif


//uncomment this to print latency breakdown in the critical path
//#define LAT_DEBUG

//uncomment this to print latency breakdown in the rpc latency
//#define RPC_LAT_DEBUG
