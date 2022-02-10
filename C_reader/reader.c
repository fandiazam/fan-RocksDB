//#include <conio.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <semaphore.h>

int numRequest = 100;
sem_t n_thread_semaphore;
int N_THD_SEMA = 4;
const static int N_THREAD = 1000;


static void* init_client_thread(void *vargp) {
    struct m0_uint128 obj_id = {0, 0};
    Arg *arg = (Arg *)vargp;

    /* Update 5th argument to create different object ids among the threads */
    obj_id.u_lo = atoll(arg->argv5)+arg->id;
    if (obj_id.u_lo < M0_ID_APP.u_lo) {
        printf("obj_id invalid. Please refer to M0_ID_APP "
               "in motr/client.c\n");
        exit(-EINVAL);
    }

    // printf("This is thread.. %d\n", arg->id);
    start_each_thread(obj_id, arg);

    pthread_exit(NULL);
}

static int start_each_thread(struct m0_uint128 obj_id, Arg *arg)
{
    struct m0_container motr_container;
    struct timeval st, et;
    int rc = 0, elapsed = 0;
    sem_wait(&n_thread_semaphore);

    m0_container_init(&motr_container, NULL, &M0_UBER_REALM, m0_instance);
    rc = motr_container.co_realm.re_entity.en_sm.sm_rc;
    if (rc != 0)
    {
        printf("error in m0_container_init: %d\n", rc);
        goto out;
    }

    if (ENABLE_WRITE > 0) {
        rc = object_create(&motr_container, obj_id);
    }
    if (rc == 0)
    {   
        if (ENABLE_WRITE > 0) {
            rc = object_write(&motr_container, arg, obj_id);
        }
        if (ENABLE_READ > 0 && rc == 0)
        {
            rc = object_read(&motr_container, arg, obj_id);
        }
        if ( ENABLE_DELETE > 0) {
            // pthread_barrier_wait(&barr_delete);
            if (ENABLE_LATENCY_LOG > 0)
                printf("Thread_%d delete .. obj_id %d\n", arg->id, obj_id.u_lo);
            object_delete(&motr_container, obj_id);
        }
    }
    sem_post(&n_thread_semaphore);

out:
    return elapsed;
}


int main () {

   //evTable = [3, 4, 12, 16, 21]
   const char * evTable3 = "/mnt/extra/c_reader/csv_workload/ev-table-3.csv";
   const char * evTable4 = "/mnt/extra/c_reader/csv_workload/ev-table-4.csv";
   const char * evTable12 = "/mnt/extra/c_reader/csv_workload/ev-table-12.csv";
   const char * evTable16 = "/mnt/extra/c_reader/csv_workload/ev-table-16.csv";
   const char * evTable21 = "/mnt/extra/c_reader/csv_workload/ev-table-21.csv";

   FILE* fp3 = fopen(evTable3,"r");
   FILE* fp4 = fopen(evTable4,"r");
   FILE* fp12 = fopen(evTable12,"r");
   FILE* fp16 = fopen(evTable16,"r");
   FILE* fp21 = fopen(evTable21,"r");

   if (!fp3 || !fp4 || !fp12 || !fp16 || !fp21) {
      printf("Can't open file\n");
      exit(1);
   }
   printf("Database is connected!");
   struct timeval st, et;
   sem_init(&n_thread_semaphore, 0, N_THD_SEMA);

   /* To print out progress every 5 % */
    if (N_THREAD > 20)
        progress_mod = N_THREAD/20;

   gettimeofday(&st, NULL);
   // Let us create three threads
   for (i = 0; i < N_THREAD; i++){
      array_arg[i].id = i+1;
      array_arg[i].argv5 = argv[5];
      array_arg[i].latency = 0;
      pthread_create(&t[i], NULL, init_client_thread, &array_arg[i]);
   }
   for (i = 0; i < N_THREAD; i++)
      pthread_join(t[i], NULL);
   
   gettimeofday(&et, NULL);


   FILE *fp;

   fp = fopen("file.txt","w+");
   fputs("This is tutorialspoint.com", fp);
  
   fseek( fp, 7, SEEK_SET );
   fputs(" C Programming Language", fp);
   fclose(fp);
   


   fclose(fp3);
   fclose(fp4);
   fclose(fp12);
   fclose(fp16);
   fclose(fp21);


   return(0);
}