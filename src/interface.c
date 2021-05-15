// interface.c
// user interface of DBMS

#include "types.h"
#include "defs.h"
#include "bpt.h"

// Allocate the buffer pool with the given number of entries.
// if success, return 0. Otherwise, return non-zero value.
// Do recovery after initalization in this function.
// Log file will be made using log_path.
// flag is needed for recovery test. 0 means normal recovery protocol.
// 1 means REDO CRASH, 2 means UNDO CRASH.
// log_num is need for REDO/UNDO crash, which means the function must return 0 after
// the number of logs is processed.
int init_db(int num_buf, int flag, int log_num, char *log_path, char *logmsg_path) {
  if (init_buffer(num_buf))
    return 1;
  int ret = log_init(log_path, logmsg_path, flag, log_num);
#ifdef  DEBUG_MODE
  //FILE *msg_fp;
  //msg_fp = fopen(logmsg_path, "r");
  //char buf[512];
  //while (fgets(buf, 512, msg_fp)) { printf("%s", buf); }
  //fclose(msg_fp);
#endif
  return ret;
}

// Destroy buffer manager.
int shutdown_db(void) {
  close_all_table();
  return 0;
}

// open existing data file using 'pathname' or
// create one if not existed
// if success, return unique table id, which represents
// the own table in this database.
// otherwise, return negative value
int open_table(char *pathname) {
  return table_open(pathname);
}

// write the pages relating to this table to disk
// and close the table.
int close_table(int table_id) {
  if (check_table(table_id)) { return -1; }
  return table_close(table_id);
}


// insert input record to data file
// if success, return 0.
// otherwise, return non-zero value.
int db_insert(int table_id, int64_t key, char *value) {
  if (check_table(table_id)) { return -1; }
  return insert(table_id, key, value);
}

// find the record and return matching value
// if found, return 0 and value string in ret_val.
// otherwise, return non-zero value.
// Memeory alloaction for record should occur in caller function.
int db_find(int table_id, int64_t key, char *ret_val, int trx_id) {
  if (check_table(table_id)) { return -1; }
  return find_trx(table_id, key, ret_val, trx_id);
}


// find the matching key and modify the value.
int db_update(int table_id, int64_t key, char *values, int trx_id) {
  if (check_table(table_id)) { return -1; }
  return update_trx(table_id, key, values, trx_id);
}


// find the record and delete it if found
// if success, return 0.
// otherwise, return non-zero value.
int db_delete(int table_id, int64_t key) {
  if (check_table(table_id)) { return -1; }
  return delete_low(table_id, key);
}
