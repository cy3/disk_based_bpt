// bpt.h
// interface for user
#include "types.h"

// interface.c
int init_db(int num_buf, int flag, int log_num, char *log_path, char *logmsg_path);
int shutdown_db(void);
int open_table(char *pathname);
int close_table(int table_id);
int db_insert(int table_id, int64_t key, char *value);
int db_find(int table_id, int64_t key, char *ret_val, int trx_id);
int db_update(int table_id, int64_t key, char *values, int trx_id);
int db_delete(int table_id, int64_t key);

// trx.c
int trx_begin();
int trx_commit(int trx_id);
int trx_abort(int trx_id);

//  for debug
int find(int id, int64_t key, char *ret_val);
void db_info(int);
