#include "types.h"
#include "defs.h"
#include "trx.h"

pthread_mutex_t trx_table_latch = PTHREAD_MUTEX_INITIALIZER;
std::unordered_map<int, trx_t> trx_table;
static int cur_trx_num = 0;

void trx_latch_lock(void) { pthread_mutex_lock(&trx_table_latch); }
void trx_latch_unlock(void) { pthread_mutex_unlock(&trx_table_latch); }


// if valid, return 1.
int trx_vaild(int trx_id) {
  trx_latch_lock();
  auto it = trx_table.find(trx_id);
  if (it == trx_table.end()) {
    trx_latch_unlock();
    return 0;
  }
  trx_latch_unlock();
  return 1;
}


// sleep on conditional variable of the lock_obj
// atomically releasing the transaction latch
void
lock_wait(lock_t *lock) {
  pthread_mutex_t *trx_mutex;
  trx_latch_lock();
  trx_mutex = &trx_table[lock->trx_id].trx_latch;
  pthread_mutex_lock(trx_mutex);
  trx_latch_unlock();
  lock_latch_unlock();
  pthread_cond_wait(&lock->cond, trx_mutex);
  pthread_mutex_unlock(trx_mutex);
}

void
trx_signal(int trx_id, pthread_cond_t *cond) {
  pthread_mutex_t *trx_mutex;
  trx_mutex = &trx_table[trx_id].trx_latch;
  pthread_mutex_lock(trx_mutex);
  pthread_cond_signal(cond);
  pthread_mutex_unlock(trx_mutex);
}

// add lock to transaction end.
int trx_add_lock(int trx_id, lock_t *lock) {
  lock_t *cur, *next;
  trx_latch_lock();

  cur = trx_table[trx_id].head;
  // first trx lock.
  if (cur == nullptr) {
    trx_table[trx_id].head = lock;
    trx_latch_unlock();
    return trx_id;
  }

  next = cur->trx_next;
  while(next != nullptr) {
    cur = next;
    next = cur->trx_next;
  }

  cur->trx_next = lock;
  lock->trx_prev = cur;
  trx_latch_unlock();
  return trx_id;
}


// allocate a transaction structure and initialize it.
// return a unique transaction id (>= 1) if success, otherwise return 0.
// allocating transaction id need to holding a mutex.
int trx_begin(void) {
  int trx_num;
  trx_latch_lock();
  cur_trx_num++;
  trx_table[cur_trx_num].head = nullptr;
  trx_table[cur_trx_num].trx_latch = PTHREAD_MUTEX_INITIALIZER;
  trx_num = cur_trx_num;
  trx_latch_unlock();
  log_add(trx_num, 0);
  return trx_num;
}

int trx_remove(int trx_id) {
  lock_t *cur, *next;
  lock_latch_lock();
  trx_latch_lock();

  auto it = trx_table.find(trx_id);
  if (it == trx_table.end()) {
    trx_latch_unlock();
    lock_latch_unlock();
    return 0;
  }
  cur = it->second.head;
  while (cur != nullptr) {
    next = cur->trx_next;
    lock_release(cur);
    cur = next;
  }
  trx_table.erase(trx_id);

  trx_latch_unlock();
  lock_latch_unlock();
  return trx_id;
}

// clean up the transaction with  given trx_id and its related information
// that has been used in lock manager. (shrinking phase of strict 2PL)
// return the completed transaction id if success, otherwise return 0.
int trx_commit(int trx_id) {
  if(trx_remove(trx_id) == 0) {
    return 0;
  }
  log_add(trx_id, 2);
  log_flush();
  return trx_id;
}


int trx_abort(int trx_id) {
  lock_t *cur, *next;

  //rollback
  trx_latch_lock();
  auto it = trx_table.find(trx_id);
  if (it == trx_table.end()) {
    trx_latch_unlock();
    return 0;
  }
  cur = it->second.head;
  trx_latch_unlock();

  while (cur != nullptr) {
    // it is first lock and it updated value.
    if (cur->prev_value != nullptr) {
      lock_latch_lock();
      struct index_t index = cur->entry->index;
      lock_latch_unlock();
      update_abort_trx(index.table_id, index.record_id, cur->prev_value, trx_id);
      free(cur->prev_value);
    }
    next = cur->trx_next;
    cur = next;
  }
  //pthread_mutex_unlock(trx_mutex);

  // remove trx from lock table
  trx_remove(trx_id);
  log_add(trx_id, 3);
  log_flush();
  return trx_id;
}
