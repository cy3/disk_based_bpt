// main.c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <inttypes.h>
#include "bpt.h"
//#define DEBUG_MODE

#define BUF_NUM 100

#define TRANSFER_THREAD_NUMBER	(8) // (8)
#define SCAN_THREAD_NUMBER		(0)

#define TRANSFER_COUNT			(30)
#define SCAN_COUNT				(30000)
//#define TRANSFER_COUNT			(1000000)
//#define SCAN_COUNT				(1000000)

#define TABLE_NUMBER			(4)
#define RECORD_NUMBER			(10)
#define INITIAL_MONEY			(100000)
#define MAX_MONEY_TRANSFERRED	(100)
#define SUM_MONEY				(TABLE_NUMBER * RECORD_NUMBER * INITIAL_MONEY)

int do_thread_test();


int test_insert(int id, int size, int inc) {
  char value[120];
  char ret[120];
  int i;

  printf(
      "============\n"
      "INSERT CHECK\n"
      "0~%d, d:%d\n"
      "============\n",size, inc);
  if (inc > 0) { i = 0; } else { i = size; }
  for (;i >= 0 && i <= size; i+=inc) {
    if(find(id, i, ret)) {
      printf("%d : not found\n", i);
    } else {
      memset(value, 0, sizeof(value));
      sprintf(value, "%d value", i);
      if (memcmp(value, ret, sizeof(value)))
        printf("%d : wrong value(%s)\n", i, ret);
    }
  }
  return 0;
}

int test_delete(int id, int size, int inc) {
  int i;

  printf(
      "============\n"
      "DELETE CHECK\n"
      "0~%d, d:%d\n"
      "============\n",size, inc);
  if (inc > 0) { i = 0; } else { i = size; }
  for (;i >= 0 && i <= size; i+=inc) {
    if(find(id, i, NULL) == 0) {
      printf("%d : delete and found\n", i);
    }
  }
  return 0;
}

int do_delete(int id, int size, int inc) {
  int i;

  printf(
      "============\n"
      "  DELETE  \n"
      "0~%d, d:%d\n"
      "============\n",size, inc);
  if (inc > 0) { i = 0; } else { i = size; }
  for (;i >= 0 && i <= size; i+=inc) {
    if(db_delete(id, i)) {
      printf("%d : delete fail\n", i);
    }
  }
  return 0;
}

int do_insert(int id, int size, int inc) {
  char value[120];
  int i;

  printf(
      "============\n"
      "   INSERT   \n"
      "0~%d, d:%d\n"
      "============\n",size, inc);
  if (inc > 0) { i = 0; } else { i = size; }
  for (;i >= 0 && i <= size; i+=inc) {
    memset(value, 0, sizeof(value));
    sprintf(value, "%d value", i);
    db_insert(id, i, value);
  }
  return 0;
}

int do_recovery_test() {
  char value[120];
  int trx1 = trx_begin();
  int trx2 = trx_begin();
  sprintf(value, "3");
  db_update(1, 1, value, trx1);
  int trx3 = trx_begin();
  int trx4 = trx_begin();
  sprintf(value, "6");
  db_update(1, 2, value, trx3);
  sprintf(value, "7");
  db_update(1, 3, value, trx2);
  sprintf(value, "8");
  db_update(1, 4, value, trx1);
  trx_commit(trx1);
  sprintf(value, "11");
  db_update(1, 4, value, trx3);
  int trx5 = trx_begin();
  sprintf(value, "13");
  db_update(1, 1, value, trx5);
  trx_commit(trx3);
  sprintf(value, "16");
  db_update(1, 4, value, trx4);
  sprintf(value, "17");
  db_update(1, 5, value, trx2);
  sprintf(value, "18");
  db_update(1, 2, value, trx5);
  trx_commit(trx4);
  sprintf(value, "21");
  db_update(1, 6, value, trx5);
  for (int i = 0; i < 20; i++) {
    sprintf(value, "21");
    db_update(1, 6, value, trx5);
  }
  return 0;
}

int main() {
    char instruction;
    int input;
    int temp;
    int id;
    char value[120];
    time_t t;

    printf("buffer number : %d\n", BUF_NUM);
    char logdata[] = "logfile.data";
    char logmsg[] = "logmsg.txt";
    init_db(BUF_NUM, 0, 0, logdata, logmsg);
    //scanf("%d", &temp);
    //init_db(temp);

    for (temp = 1; temp < TABLE_NUMBER; ++temp) {
      sprintf(value, "DATA%d", temp);
      open_table(value);
    }
    memset(value, 0, sizeof(value));
    for (int i = 1; i < 7; ++i) {
      sprintf(value, "%d", i);
      db_insert(1, i, value);
    }
    for (temp = 1; temp < TABLE_NUMBER; ++temp) {
      close_table(temp);
    }
    for (temp = 1; temp < TABLE_NUMBER; ++temp) {
      sprintf(value, "DATA%d", temp);
      open_table(value);
    }
    //while (getchar() != (int)'\n');
    //setvbuf (stdout, NULL, _IONBF, BUFSIZ);

    for(;;) {
        printf("> ");
        scanf("%c", &instruction);
        switch (instruction) {
          case 'l':
            do_recovery_test();
            break;
        case 'd':
            scanf("%d %d", &id, &input);
            db_delete(id, input);
            break;
        case 'i':
            memset(value, 0, 120);
            scanf("%d %d %s", &id, &input, value);
            if(db_insert(id, input, value)) {
              printf("insert fail\n");
            }
            break;
        case 'f':
            scanf("%d %d", &id, &input);
            if(find(id, input, value)) {
              printf("not found\n");
            } else {
              printf("%d: %s\n", input, value);
            }
            break;
        case 't':
            scanf("%d %d %d", &id, &input, &temp);

            t = time(NULL);
            do_insert(id, input, temp);
            t = time(NULL)-t;
            printf("insert: %d sec\n", (int)t);

            t = time(NULL);
            test_insert(id, input, temp);
            t = time(NULL)-t;
            printf("check: %d sec\n", (int)t);
            break;
        case 'e':
            scanf("%d %d %d", &id, &input, &temp);

            t = time(NULL);
            do_delete(id, input, temp);
            t = time(NULL)-t;
            printf("delete: %d sec\n", (int)t);

            t = time(NULL);
            test_delete(id, input, temp);
            t = time(NULL)-t;
            printf("check: %d sec\n", (int)t);
            break;
        case 'x':
            do_thread_test();
            break;
        case 'r':
            scanf("%d", &input);
            db_info(input);
            break;
        case 'q':
            shutdown_db();
            return 0;
        default:
            break;
        }
        while (getchar() != (int)'\n');
    }
}

/*
 * This thread repeatedly transfers some money between accounts randomly.
 */

void*
transfer_thread_func(void* arg)
{
	int				source_table_id;
	int				source_record_id;
	int				destination_table_id;
	int				destination_record_id;
	int				money_transferred;

	for (int i = 0; i < TRANSFER_COUNT; i++) {
		/* Decide the source account and destination account for transferring. */
		source_table_id = rand() % TABLE_NUMBER;
		source_record_id = rand() % RECORD_NUMBER;
		destination_table_id = rand() % TABLE_NUMBER;
		destination_record_id = rand() % RECORD_NUMBER;

    //if ((source_table_id > destination_table_id) ||
		//		(source_table_id == destination_table_id &&
		//		 source_record_id >= destination_record_id)) {
		//	/* Descending order may invoke deadlock conditions, so avoid it. */
		//	continue;
		//}
		
		/* Decide the amount of money transferred. */
		money_transferred = rand() % MAX_MONEY_TRANSFERRED;
		money_transferred = rand() % 2 == 0 ?
			(-1) * money_transferred : money_transferred;
		
    int trx_id;
    int temp;
    char value[120];
    trx_id = trx_begin();
    // withdraw
    db_find(source_table_id+1, source_record_id, value, trx_id);
    temp = atoi(value);
    sprintf(value, "%d", temp-money_transferred);
    db_update(source_table_id+1, source_record_id, value, trx_id);

    // deposit
    db_find(destination_table_id+1, destination_record_id, value, trx_id);
    temp = atoi(value);
    sprintf(value, "%d", temp+money_transferred);
    db_update(destination_table_id+1, destination_record_id, value, trx_id);

    trx_commit(trx_id);
	}

	printf("Transfer thread is done.\n");

	return NULL;
}

/*
 * This thread repeatedly check the summation of all accounts.
 * Because the locking strategy is 2PL (2 Phase Locking), the summation must
 * always be consistent.
 */
void*
scan_thread_func(void* arg)
{
	int				sum_money;

	for (int i = 0; i < SCAN_COUNT; i++) {
		sum_money = 0;

		/* Iterate all accounts and summate the amount of money. */
    int trx_id;
    int temp;
    char value[120];
    trx_id =  trx_begin();
		for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
			for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
        db_find(table_id+1, record_id, value, trx_id);
        temp = atoi(value);
        sum_money += temp;
			}
		}
    if (trx_commit(trx_id)) {
      /* Check consistency. */
      if (sum_money != SUM_MONEY) {
        printf("Inconsistent state is detected!!!!!\n");
        printf("sum_money : %d\n", sum_money);
        printf("SUM_MONEY : %d\n", SUM_MONEY);
        return NULL;
      }
    } else {
      //printf("Scan aborted.\n");
    }
	}

	printf("Scan thread is done.\n");

	return NULL;
}

int do_thread_test()
{
	pthread_t	transfer_threads[TRANSFER_THREAD_NUMBER];
	pthread_t	scan_threads[SCAN_THREAD_NUMBER];

	srand(time(NULL));

	/* Initialize accounts. */
  char value[120];
  sprintf(value, "%d", INITIAL_MONEY);
	for (int table_id = 0; table_id < TABLE_NUMBER; table_id++) {
		for (int record_id = 0; record_id < RECORD_NUMBER; record_id++) {
      db_delete(table_id+1, record_id);
      db_insert(table_id+1, record_id, value);
		}
	}

	/* thread create */
	for (int i = 0; i < TRANSFER_THREAD_NUMBER; i++) {
		pthread_create(&transfer_threads[i], 0, transfer_thread_func, NULL);
	}
	for (int i = 0; i < SCAN_THREAD_NUMBER; i++) {
		pthread_create(&scan_threads[i], 0, scan_thread_func, NULL);
	}

	/* thread join */
	for (int i = 0; i < TRANSFER_THREAD_NUMBER; i++) {
		pthread_join(transfer_threads[i], NULL);
	}
	for (int i = 0; i < SCAN_THREAD_NUMBER; i++) {
		pthread_join(scan_threads[i], NULL);
	}

  for (int temp = 0; temp < TABLE_NUMBER; ++temp) {
    close_table(temp+1);
    sprintf(value, "%d.db", temp);
    open_table(value);
  }

	return 0;
}

