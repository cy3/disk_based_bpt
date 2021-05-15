// standard lib
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "types.h"
//#define DEBUG_MODE

struct page_t;
struct lock_t;
struct lpage;
struct record;
struct buffer;

// find.c
pagenum_t find_leaf(int id, int64_t key);
int find_index(int id, pagenum_t parent, pagenum_t child);
int find_record(struct lpage *leaf, int64_t key);
int find(int id, int64_t key, char *ret_val);

// find_trx.cc
int find_trx(int id, int64_t key, char *ret_val, int trx_id);
int update_trx(int table_id, int64_t key, char *values, int trx_id);
int update_abort_trx(int table_id, int64_t key, char *values, int trx_id);

// insert.c
int insert(int id, int64_t key, char *value);

// delete.c
int delete_low(int id, int64_t key);

// page.c
struct hpage *get_hpage(int);
struct fpage *get_fpage(int, pagenum_t);
struct ipage *get_ipage(int, pagenum_t);
struct lpage *get_lpage(int, pagenum_t);

pagenum_t alloc_page(int);
void free_page(int, pagenum_t);
void init_head_page(int);
pagenum_t get_root_num(int);
pagenum_t start_root_page(int);
int set_root_page(int, pagenum_t);
int set_page_parent(int, pagenum_t child, pagenum_t parent);

// buffer.c
int init_buffer(int);
void close_buffer(void);
page_t *get_page(int, pagenum_t);
void set_dirty_page(int, pagenum_t);
void unpin_page(int, pagenum_t);
void flush_table(int);
struct buffer *get_buffer(int id, pagenum_t num);
void set_dirty_buffer(struct buffer *buf);
void unlock_buffer(struct buffer *buf);

// table.c
int table_open(char *pathname);
int table_close(int);
void close_all_table();
int check_table(int);
int find_fd(int);

// file.c
int file_open(char *pathname);
int file_close(int);
int file_size(int);
void file_extend(int, pagenum_t, int);
void file_read_page(int, pagenum_t pagenum, struct page_t *dest);
void file_write_page(int, pagenum_t, const struct page_t *src);

// log.cc
int log_init(char *log_path, char *logmsg_path, int flag, int log_num);
int log_flush();

// lock.cc
void lock_latch_lock();
void lock_latch_unlock();
int lock_release(lock_t *);
int init_lock_table();
int lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode, lock_t *&);

// trx.cc
void trx_latch_lock();
void trx_latch_unlock();
int trx_vaild(int);
int trx_add_lock(int trx_id, lock_t *lock);
void lock_wait(lock_t *);
void trx_signal(int, pthread_cond_t *);
int trx_abort(int trx_id);

// log.cc
int log_init(char *log_path, char *logmsg_path, int flag, int log_num);
int log_flush();
uint64_t log_add(int trx_id, int type);
uint64_t log_update(int, int, int, pagenum_t, int, int, char *, char *, uint64_t);

//debug.c
void panic(const char *str);
void db_info(int);
