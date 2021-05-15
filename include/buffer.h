// buffer.h
#include <pthread.h>
#include "types.h"

struct buffer {
  page_t frame; // data of target page
  int table_id; // the unique id of table
  pagenum_t page_num; // target page number within a file
  uint is_dirty; // 1 = dirty, 0 = clean
  uint pin_count; // current number of access
  pthread_mutex_t page_latch;
  struct buffer *next; // next buffer
  struct buffer *prev; // prev buffer
};

struct lru {
  struct buffer *head;
  struct buffer *tail;
};
