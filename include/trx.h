#include "lock.h"

struct trx_t {
  struct lock_t *head;
  pthread_mutex_t trx_latch;
};
