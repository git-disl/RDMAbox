#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <syslog.h>
#include <pthread.h>

//int bytes = (1024*1024);
int bytes = (10*1024*1024);
//int bytes = (20*1024*1024 + 512);

void test_fn(void *arg)
{
   FILE *fptr;
   int num_repeat = 1000;
   struct timeval tval_before, tval_after, tval_result;
   char *data = arg;

   fptr = fopen("/users/jbae91/ramdisktest/program.txt","w");

   gettimeofday(&tval_before, NULL);
   for(int i; i < num_repeat; i++){
       fwrite(data, bytes, 1, fptr);
   }
   gettimeofday(&tval_after, NULL);

   timersub(&tval_after, &tval_before, &tval_result);
   printf("Repeat write done. Time elapsed: %ld.%06ld\n", (long int)tval_result.tv_sec, (long int)tval_result.tv_usec);
   fclose(fptr);
}
int main()
{
   int num;
   FILE *fptr;
   FILE *fptr2;
   FILE *fptr3;
   int i;
   struct timeval tval_before, tval_after, tval_result;
   char *data;
   char *data2;

   pthread_t test_thread;

   setlogmask (LOG_UPTO (LOG_NOTICE));
   openlog ("exampleprog", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);

   data = (char *) malloc(bytes);
   data2 = (char *) malloc(bytes);

   for(i=0;i<bytes;i++){
        data2[i] = (char) 'C';
   }

   for(i=0;i<bytes;i++){
        data[i] = (char) i;
   }

   fptr2 = fopen("/users/jbae91/ramdisktest/program.txt","w");

   gettimeofday(&tval_before, NULL);
   fwrite(data, bytes, 1, fptr2);
   gettimeofday(&tval_after, NULL);

   timersub(&tval_after, &tval_before, &tval_result);
   printf("Write done. Time elapsed: %ld.%06ld\n", (long int)tval_result.tv_sec, (long int)tval_result.tv_usec);
   fclose(fptr2);

   fptr3 = fopen("/users/jbae91/ramdisktest/program.txt","r");

   gettimeofday(&tval_before, NULL);
   fread(data2, bytes, 1, fptr3);
   gettimeofday(&tval_after, NULL);

   timersub(&tval_after, &tval_before, &tval_result);
   printf("Read done. Time elapsed: %ld.%06ld\n", (long int)tval_result.tv_sec, (long int)tval_result.tv_usec);
   fclose(fptr3);

   if(strcmp(data, data2) == 0){
       printf("match\n");
   }
   else{
       printf("mismatch\n");
       for(i=0;i<bytes;i++){
         if( data2[i] != data[i]){
             printf("mismatch data[%d]=%c\n",i,data2[i]);
	     break;
	 }
       }
   }

/*
   // repeat test //
   for(i=0;i<10;i++) {
       pthread_create(&test_thread, NULL, test_fn, data);
   }
*/

   return 0;
}
