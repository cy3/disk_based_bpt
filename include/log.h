// log.h
#include <stdint.h>
#include <pthread.h>
#include <unordered_map>
#include <algorithm>
#include <unordered_set>
#include <vector>
#include <list>

#define LOG_BUF_SIZE (4000000)
#define LOG_HEADER_SIZE (28)
#define LOG_UPDATE_SIZE (288)
#define LOG_COMPENSATE_SIZE (296)

// size = 292
struct log_t {
  uint64_t lsn;
  uint64_t prev_lsn;
  int log_size;
  int trx_id;
  int type;
  int table_id;
  pagenum_t pagenum;
  int offset;
  int len;
  char old_image[VALUE_SIZE];
  char new_image[VALUE_SIZE];
  uint64_t next_undo_lsn;
};

