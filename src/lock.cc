#include "types.h"
#include "defs.h"
#include "lock.h"

pthread_mutex_t lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
std::unordered_map<index_t, entry_t> hash_table;
static int cur_lock_num = 0;

void lock_latch_lock(void) { pthread_mutex_lock(&lock_table_latch); }
void lock_latch_unlock(void) { pthread_mutex_unlock(&lock_table_latch); }

size_t std::hash<index_t>::operator() (const index_t& i) const {
  return  hash<int>()(i.table_id) ^
          hash<int64_t>()(i.record_id);
}

bool std::equal_to<index_t>::operator()
  (const index_t& lhs, const index_t& rhs) const {
  if (lhs.table_id != rhs.table_id ||
      lhs.record_id != rhs.record_id) {
    return false;
  } else {
    return true;
  }
}

// Initalize data structure for lock table.
// hash table, lock table latch..
// If success, return 0.
// Otherwise, return non-zero value.
int init_lock_table() { return 0; }


// debug function
void
print_table(int max) {
  //auto it = hash_table.begin();
  //while(it != hash_table.end()) {
    //auto entry = it->second;
  for (int i = 0; i < max; ++i) {
    struct index_t index;
    index.table_id = 1;
    index.record_id = i;
    auto it = hash_table.find(index);
    auto entry = it->second;

    if (entry.head != nullptr) {
      printf("(%d %d)\t",  entry.index.table_id, (int)entry.index.record_id);
      auto cur = entry.head;
      while (cur != nullptr) {
        if (cur->lock_mode == 1) {
          printf("x:");
        } else {
          printf("s:");
        }
        if  (cur->trx_next == nullptr) {
          printf("(%d)\t\t", cur->trx_id);
        } else {
          if (cur->trx_prev == nullptr) {printf("*");}
          printf("%d -> (%d,%d)\t", cur->trx_id, 
             cur->trx_next->entry->index.table_id,
             (int)cur->trx_next->entry->index.record_id);
        }

        cur = cur->next;
      }
      printf("\n");
    }
    //it++;
    //
  }
}

int
add_edge_table(
    std::list<std::pair<int, int>> &edge_table,
    int trx_id, int trx_prev) {
  auto edge = std::make_pair(trx_id, trx_prev);
  auto it = std::find(edge_table.begin(), edge_table.end(), edge);
  if (it == edge_table.end()) {
    edge_table.push_back(edge);
  }
  return 0;
}

int
build_wait_graph(
    std::unordered_map<int, int> &table,
    std::list<std::pair<int, int>> &edge_table,
    lock_t *lock) {

  auto it = table.find(lock->lock_id);
  if (it != table.end()) {
    return it->second;
  }

  table[lock->lock_id] = 0;

  if (lock->prev == nullptr && lock->trx_next == nullptr) {
    return 0;
  }

  if (lock->trx_next != nullptr) {
    build_wait_graph(table, edge_table, lock->trx_next);
  }

  lock_t *prev = lock->prev;
  int lock_mode = lock->lock_mode;
  while (prev != nullptr) {
    if (prev->lock_mode == 1 || lock_mode == 1) {
      add_edge_table(edge_table, lock->trx_id, prev->trx_id);
      lock_mode = 1;
    }
    build_wait_graph(table, edge_table, prev);
    prev = prev->prev;
  }

  return 0;
}

int
cycle_find(
  std::list<std::pair<int, int>> &trx_edge,
  int trx_new) {

  std::vector<int> depen;
  for (auto it = trx_edge.begin(); it != trx_edge.end(); ++it) {
    if (it->first == trx_new) {
      depen.emplace_back(it->second);
    }
  }
  bool flag = true;
  while (flag) {
    flag = false;
    for (auto it = trx_edge.begin(); it != trx_edge.end(); ++it) {
      auto result1 = std::find(depen.begin(), depen.end(), it->first);
      auto result2 = std::find(depen.begin(), depen.end(), it->second);
      if (result1 != depen.end() && result2 == depen.end()) {
          depen.emplace_back(it->second);
          flag = true;
      }
    }
  }
  // deadlock detected
  auto result = std::find(depen.begin(), depen.end(), trx_new);
  if (result != depen.end()) {
      return 1;
    }
  return 0;
}


int
check_deadlock(lock_t *lock, int trx_new, int lock_mode) {
  trx_latch_lock();

  std::unordered_map<int, int> table;
  std::list<std::pair<int, int>> edge_table;
  build_wait_graph(table, edge_table, lock);
  if (cycle_find(edge_table, trx_new)) {
    trx_latch_unlock();
    return 1;
  }
  trx_latch_unlock();
  return 0;
}


lock_t*
init_lock(struct entry_t *entry, int trx_id, int lock_mode) {
  lock_t *lock = (lock_t *)malloc(sizeof(lock_t));
  lock->prev = nullptr;
  lock->next = nullptr;
  lock->entry = entry;
  lock->lock_id = ++cur_lock_num;
  lock->trx_id = trx_id;
  lock->cond = PTHREAD_COND_INITIALIZER;
  lock->lock_mode = lock_mode;
  lock->prev_value = nullptr;
  lock->trx_prev = nullptr;
  lock->trx_next = nullptr;
  return lock;
}

lock_t*
find_lock(struct entry_t *entry, int trx_id) {
  lock_t *cur;
  cur = entry->head;

  while (cur != nullptr) {
    if (cur->trx_id == trx_id) { return cur; }
    cur = cur->next;
  }
  return nullptr;
}


int
lock_remove(lock_t *lock) {
  struct entry_t *entry = lock->entry;

  // remove lock from table
  if (entry->head == entry->tail) {
    // last one lock
    entry->head = nullptr;
    entry->tail = nullptr;
  } else {
    // remove and wake up next
    if (lock->prev == nullptr) {
      entry->head = lock->next;
      if (lock->next != nullptr) {
        lock->next->prev = nullptr;
      }
    } else {
      lock->prev->next = lock->next;
      if (lock->next != nullptr) {
        lock->next->prev = lock->prev;
      }
    }
    if (lock->next == nullptr) {
      entry->tail = lock->prev;
    }
  }
  free(lock);
  return 0;
}


// Remove the lock_obj from the lock list.
// If there is a successor's lock waiting for the thread releasing
// the lock, wake up the successor.
// If success, return 0.
// Otherwise, return non-zero value.
int
lock_release(lock_t* lock)
{
  struct entry_t *entry = lock->entry;

  lock_remove(lock);

  if (entry->head != nullptr) {
    // wake up all s lock.
    if (entry->head->lock_mode == lock_mode_t::SHARED) {
      lock_t *cur = entry->head;
      while (cur != nullptr && cur->lock_mode == lock_mode_t::SHARED) {
        trx_signal(cur->trx_id, &cur->cond);
        cur = cur->next;
      }
    }
    // wake up one x lock.
    if (entry->head->lock_mode == lock_mode_t::EXCLUSIVE) {
      trx_signal(entry->head->trx_id, &entry->head->cond);
    }
  }
  return 0;
}


// Allocate and append a new lock object to lock list
// of the record having the key.
// If there is a predecessor's lock object in the lock list,
// sleep until the predecessor to release its lock.
// If there is no predecessor's lock table, return the
// address of the new lock object.
// If an error occurs, return NULL.
// lock_mode : 0 (shared) or 1 (exclusive)
int
lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode, lock_t *&ret_lock)
{
  pthread_mutex_lock(&lock_table_latch);

  struct index_t index;
  index.table_id = table_id;
  index.record_id = key;
  auto it = hash_table.find(index);

  if (it == hash_table.end()) {
    // insert new entry
    struct entry_t entry;
    entry.index = index;
    entry.tail = nullptr;
    entry.head = nullptr;
    auto out = hash_table.emplace(index, entry);
    it = out.first;
  }

  // add lock to entry.
  struct entry_t *entry = &it->second;

  // no waiting.
  if (entry->head == nullptr) {
    lock_t *lock = init_lock(entry, trx_id, lock_mode);

    entry->head = lock;
    entry->tail = lock;
    trx_add_lock(trx_id, lock);
    pthread_mutex_unlock(&lock_table_latch);
    ret_lock = lock;
    return lock_code_t::ACQUIRED;
  }

  // check lock is already exist
  // that means current trx is running.
  lock_t *cur = find_lock(entry, trx_id);
  if (cur != nullptr) {
    // s - x
    if (cur->lock_mode == lock_mode_t::SHARED
        && lock_mode == lock_mode_t::EXCLUSIVE) {
      lock_t *next = entry->head;
      while (next != nullptr) {
        if (next->trx_id != trx_id) {
#ifdef DEBUG_MODE
          //printf("s-x conflict detected. trx abort\n");
#endif
          pthread_mutex_unlock(&lock_table_latch);
          ret_lock = nullptr;
          return lock_code_t::DEADLOCK;
        }
        next = next->next;
      }
      // update lock to x lock.
      cur->lock_mode = lock_mode_t::EXCLUSIVE;
    }
    // s - s, x - s, x - x
    pthread_mutex_unlock(&lock_table_latch);
    ret_lock = cur;
    return lock_code_t::ACQUIRED;
  }

  // add new entry to table
  lock_t *lock = init_lock(entry, trx_id, lock_mode);
  lock->prev = entry->tail;

  entry->tail->next = lock;
  entry->tail = lock;
  
  // check deadlock.
  if (check_deadlock(lock, trx_id, lock_mode)) {
#ifdef DEBUG_MODE
          //printf("deadlock cycle detected. trx abort\n");
#endif
    lock_remove(lock);
    pthread_mutex_unlock(&lock_table_latch);
    ret_lock = nullptr;
    return lock_code_t::DEADLOCK;
  }
  trx_add_lock(trx_id, lock);

  // Check if this lock can be run right now.
  cur = entry->head;
  while (cur != nullptr) {
    if (cur->lock_mode == lock_mode_t::EXCLUSIVE) {
      // sleeps until predecessor wakes it up
      ret_lock = lock;
      return lock_code_t::NEED_TO_WAIT;
    }
    cur = cur->next;
  }

  pthread_mutex_unlock(&lock_table_latch);
  ret_lock = lock;
  return lock_code_t::ACQUIRED;
}
