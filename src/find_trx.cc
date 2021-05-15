// find_trx.cc
// index layer of DBMS
// with trx lock system.

#include "types.h"
#include "defs.h"
#include "page.h"
#include "buffer.h"
#include "trx.h"

pagenum_t get_root_trx(int id) {
  struct buffer *buf;
  struct hpage *head;
  pagenum_t r;

  buf = get_buffer(id, HPAGE_NUM);
  head = (struct hpage *)(&buf->frame);
  r = head->root;
  unlock_buffer(buf);
  return r;
}

// Traces the path from root to a leaf.
// Returns the leaf page num may containing the given key.
// if there is no root, return 0
struct buffer *
find_leaf_trx(int id, int64_t key) {
  struct buffer *buf;
  struct ipage *node;
  int i;
  pagenum_t node_num, child_num;

  node_num = get_root_trx(id);
  // case : no root
  if (node_num == 0)
    return nullptr;

  buf = get_buffer(id, node_num);
  node = (struct ipage *)(&buf->frame);
  while (node->header.is_leaf == 0) {
    for (i = 0; i < node->header.num_keys; ++i) {
      if (key < node->index[i].key) { break; }
    }
    if (i == 0) {
      child_num = node->header.leftmost;
    } else {
      child_num = node->index[i-1].pagenum;
    }
    unlock_buffer(buf);
    node_num = child_num;
    buf = get_buffer(id, node_num);
    node = (struct ipage *)(&buf->frame);
  }
  return buf;
}


// find record in leaf page
// return record's index
// if not found, return nonzero value
int find_record_trx(struct lpage *leaf, int64_t key) {
  int i;
  for (i = 0; i < leaf->header.num_keys; ++i) {
    if (leaf->record[i].key == key) {
      return i;
    }
  }
  return -1;
}


// check trx and key existance.
int
find_check(int id, int64_t key, char *ret_val, int trx_id,
    struct buffer *&buf, struct lpage *&leaf,
    pagenum_t &leaf_num, int &i) {

  // check vaild trx.
  if (!trx_vaild(trx_id)) {
    return 1;
  }
  // check vaild ret.
  if(ret_val == nullptr) {
    return 1;
  }

  // get leaf page
  buf = find_leaf_trx(id, key);
  leaf_num = buf->page_num;
  if (buf == nullptr) { 
    unlock_buffer(buf);
    return 1;
  }

  // check record is exist
  leaf = (struct lpage *)(&buf->frame);
  i = find_record(leaf, key);
  if (i == -1) {
    unlock_buffer(buf);
    return 1;
  }
  return 0;
}


// find
// return nonzero if fail.
// if found, copy value to ret_val.
int
find_trx(int id, int64_t key, char *ret_val, int trx_id) {
  lock_t *lock;
  struct buffer *buf;
  struct lpage *leaf;
  pagenum_t leaf_num;
  int i = 0;

  if (find_check(id, key, ret_val, trx_id, buf, leaf, leaf_num, i)) {
    return 1;
  }

  // acquire shared record lock.
  int ret = lock_acquire(id, key, trx_id, 0, lock);
  if (ret == lock_code_t::ACQUIRED) {
    // we are first thread of record.
  } else if (ret == lock_code_t::NEED_TO_WAIT) {
    unlock_buffer(buf);
    lock_wait(lock);
    // trx latch aquired.
    buf = get_buffer(id, leaf_num);
    leaf = (struct lpage *)(&buf->frame);
  } else if (ret == lock_code_t::DEADLOCK) {
    unlock_buffer(buf);
    trx_abort(trx_id);
    return 1;
  }

  memcpy(ret_val, leaf->record[i].value, VALUE_SIZE);
  unlock_buffer(buf);
  return 0;
}

int
update_trx(int id, int64_t key, char *values, int trx_id) {
  struct buffer *buf;
  struct lpage *leaf;
  lock_t *lock;
  pagenum_t leaf_num;
  char *prev_value;
  int i = 0;

  if (find_check(id, key, values, trx_id, buf, leaf, leaf_num, i)) {
    return 1;
  }

  // acquire exclusive record lock.
  int ret = lock_acquire(id, key, trx_id, 1, lock);
  if (ret == lock_code_t::ACQUIRED) {
    // we are first thread of record.
  } else if (ret == lock_code_t::NEED_TO_WAIT) {
    unlock_buffer(buf);
    lock_wait(lock);
    // trx latch aquired.
    buf = get_buffer(id, leaf_num);
    leaf = (struct lpage *)(&buf->frame);
  } else if (ret == lock_code_t::DEADLOCK) {
    unlock_buffer(buf);
    trx_abort(trx_id);
    return 1;
  }

  // now we are first thread of record.

  // add log.
  int offset = sizeof(struct page_header) + sizeof(struct record) * i + sizeof(int64_t);
  uint64_t lsn = log_update(trx_id, 1, id, leaf_num, offset, VALUE_SIZE,
      leaf->record[i].value, values, 0);

  set_dirty_buffer(buf);
  leaf->header.page_lsn = lsn;

  if (lock->prev_value == NULL) {
    prev_value = (char *)malloc(sizeof(char)*VALUE_SIZE);
    memcpy(prev_value, leaf->record[i].value, VALUE_SIZE);
    lock->prev_value = prev_value;
  }
  memcpy(leaf->record[i].value, values, VALUE_SIZE);

  unlock_buffer(buf);
  return 0;
}


// not get page latch
int
update_abort_trx(int id, int64_t key, char *values, int trx_id) {
  struct buffer *buf;
  struct lpage *leaf;
  lock_t *lock;
  pagenum_t leaf_num;
  int i = 0;

  if (find_check(id, key, values, trx_id, buf, leaf, leaf_num, i)) {
    return 1;
  }

  // add log.
  int offset = sizeof(struct page_header) + sizeof(struct record) * i + sizeof(int64_t);
  uint64_t lsn = log_update(trx_id, 1, id, leaf_num, offset, VALUE_SIZE,
      leaf->record[i].value, values, 0);

  set_dirty_buffer(buf);
  leaf->header.page_lsn = lsn;

  memcpy(leaf->record[i].value, values, VALUE_SIZE);
  unlock_buffer(buf);
  return 0;
}
