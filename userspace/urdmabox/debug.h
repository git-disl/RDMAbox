static int debug = 0;
#define DEBUG_LOG if (debug) syslog
#define DEBUG_LINE if (debug) syslog(LOG_NOTICE,"[%s][%s +%d]\n",__func__,__FILE__,__LINE__);
//#define DEBUG_LINE printf("[%s][%s +%d] \n",__func__,__FILE__,__LINE__);


