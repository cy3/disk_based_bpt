#include <stdint.h>
#include <pthread.h>
#include <unordered_map>
#include <list>
#include <vector>
#include <utility>
#include <algorithm>

struct lock_t;

enum lock_code_t { ACQUIRED, NEED_TO_WAIT, DEADLOCK };
enum lock_mode_t { SHARED, EXCLUSIVE };

struct index_t {
  int table_id;
  int64_t record_id;
};

struct entry_t {
  struct index_t index;
  struct lock_t *head;
  struct lock_t *tail;
};

typedef struct lock_t {
  struct lock_t *prev;
  struct lock_t *next;
  struct entry_t *entry;
  pthread_cond_t cond;
  int lock_id;
  int trx_id;
  int lock_mode;
  struct lock_t *trx_prev;
  struct lock_t *trx_next;
  char *prev_value;
} lock_t;

namespace std
{
  template <> struct hash<index_t> {
    size_t operator() (const index_t& i) const;
  };
  template <> struct equal_to<index_t> {
    bool operator() (const index_t& lhs, const index_t& rhs) const;
  };
};
