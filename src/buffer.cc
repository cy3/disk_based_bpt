// buffer.c
// buffer layer of DBMS

#include "types.h"
#include "defs.h"
#include "buffer.h"

static int max_buf_num;
static int buf_num;
static struct lru lru;

pthread_mutex_t buffer_mgr_latch = PTHREAD_MUTEX_INITIALIZER;


// allocate and initalize buffer pool
// if success, return 0. otherwise, return non-zero value.
int init_buffer(int num) {
  pthread_mutex_lock(&buffer_mgr_latch);

  if (num < 1) {
    pthread_mutex_unlock(&buffer_mgr_latch);
    return 1;
  }
  if (max_buf_num > 0) {
    pthread_mutex_unlock(&buffer_mgr_latch);
    return 0;
  }
  max_buf_num = num;
  buf_num = 0;
  lru.head = NULL;
  lru.tail = NULL;
  pthread_mutex_unlock(&buffer_mgr_latch);
  return 0;
}


// find buffer struct of given page number.
// if fail, return NULL.
struct buffer *find_buf(int id, pagenum_t num) {
  struct buffer *next;

  next = lru.tail;
  while(next != NULL) {
    if (next->table_id == id &&
        next->page_num == num) { return next; }
    next = next->prev;
  }
  return NULL;
}


// update buffer in lru.
void update_lru(struct buffer *buf) {
  // case: add fist buffer
  if (lru.head == NULL) {
    lru.head = buf;
    lru.tail = buf;
    return;
  }

  // case: update last buffer
  if (lru.tail == buf) {
    return;
  }

  // case: add new buffer
  if (buf->next == buf->prev) {
    lru.tail->next = buf;
    buf->prev = lru.tail;
    lru.tail = buf;
    return;
  }

  // case: update buffer
  if (buf->prev != NULL)
    buf->prev->next = buf->next;
  if (buf->next != NULL)
    buf->next->prev = buf->prev;
  if (lru.head == buf)
    lru.head = buf->next;
  lru.tail->next = buf;
  buf->prev = lru.tail;
  buf->next = NULL;
  lru.tail = buf;
  return;
}


void delete_lru(struct buffer *buf) {
  if (buf->prev != NULL)
    buf->prev->next = buf->next;
  if (buf->next != NULL)
    buf->next->prev = buf->prev;
  if (lru.head == buf)
    lru.head = buf->next;
  if (lru.tail == buf)
    lru.tail = buf->prev;
  return;
}


void flush_buf(struct buffer *buf) {
  if (buf->is_dirty) {
    log_flush();
    file_write_page(find_fd(buf->table_id),
        buf->page_num, &buf->frame);
    buf->is_dirty = 0;
  }
}

// find proper space for new page by LRU policy.
// return in-memory buffer struct.
// if fail, return negative value.
struct buffer *evict_buf() {
  struct buffer *next;

  if (buf_num < max_buf_num) {
    next = (struct buffer *)malloc(sizeof(struct buffer));
    next->page_latch = PTHREAD_MUTEX_INITIALIZER;
    buf_num++;
    return next;
  }

  next = lru.head;
  while (next != NULL) {
    if (next->pin_count == 0) {
      flush_buf(next);
      delete_lru(next);
      return next;
    }
    next = next->next;
  }
  panic("evict_buf");
  return nullptr;
}


// close all page in table
void flush_table(int id) {
  struct buffer *cur;
  struct buffer *next;

  pthread_mutex_lock(&buffer_mgr_latch);
  cur = lru.tail;
  while (cur != NULL) {
    next = cur->prev;
    if (cur->table_id == id) {
      // TODO : lock page
      flush_buf(cur);
      delete_lru(cur);
      free(cur);
      buf_num--;
    }
    cur = next;
  }
  pthread_mutex_unlock(&buffer_mgr_latch);
}


// destroy buffer manager.
void close_buffer(void) {
  struct buffer *cur;
  struct buffer *next;

  pthread_mutex_lock(&buffer_mgr_latch);
  cur = lru.tail;
  while (cur != NULL) {
    // TODO: lock page
    next = cur->prev;
    flush_buf(cur);
    delete_lru(cur);
    free(cur);
    cur = next;
  }
  buf_num = 0;
  pthread_mutex_unlock(&buffer_mgr_latch);
}


// get page from buffer pool.
// caller should call unpin_page() or free_page()
// when finished using.
page_t *get_page(int id, pagenum_t num) {
  struct buffer *buf;
  buf = find_buf(id, num);

  // not found. add one.
  if (buf == NULL) {
    buf = evict_buf();
    if (buf == NULL) { panic("get_page"); }
    buf->is_dirty = 0;
    buf->pin_count = 0;
    buf->next = nullptr;
    buf->prev = nullptr;
    file_read_page(find_fd(id), num, &buf->frame);
    buf->table_id = id;
    buf->page_num = num;
  }

  buf->pin_count++;
  update_lru(buf);
  return &buf->frame;
}

// set dirty bit.
void set_dirty_page(int id, pagenum_t num) {
  struct buffer *buf;
  buf = find_buf(id, num);
  if (buf == NULL) { panic("set_dirty_page"); }
  buf->is_dirty = 1;
}


// unpin page.
// if there is no page, do nothing.
void unpin_page(int id, pagenum_t num) {
  struct buffer *buf;
  buf = find_buf(id, num);
  if (buf == NULL) {
    return;
  }
  buf->pin_count--;
}


struct buffer *
find_free_buf() {
  struct buffer *next;

  if (buf_num < max_buf_num) {
    next = (struct buffer *)malloc(sizeof(struct buffer));
    next->page_latch = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&next->page_latch);
    buf_num++;
    return next;
  }

  next = lru.head;
  if (next->pin_count == 0) {
    pthread_mutex_lock(&next->page_latch);
    flush_buf(next);
    delete_lru(next);
    return next;
  }
  //while (next != NULL) {
  //  if (next->pin_count == 0) {
  //    if(pthread_mutex_trylock(&next->page_latch) == 0) {
  //      flush_buf(next);
  //      delete_lru(next);
  //      return next;
  //    }
  //  }
  //  next = next->next;
  //}
  panic("find_free_buf");
}


struct buffer *
get_buffer(int id, pagenum_t num) {
  struct buffer *buf;

  pthread_mutex_lock(&buffer_mgr_latch);
  buf = find_buf(id, num);

  // not found. add one.
  if (buf == NULL) {
    buf = find_free_buf();
    if (buf == NULL) { panic("get_buffer"); }
    buf->is_dirty = 0;
    buf->pin_count = 0;
    buf->next = nullptr;
    buf->prev = nullptr;
    file_read_page(find_fd(id), num, &buf->frame);
    buf->table_id = id;
    buf->page_num = num;
  } else {
    pthread_mutex_lock(&buf->page_latch);
  }

  update_lru(buf);
  pthread_mutex_unlock(&buffer_mgr_latch);
  return buf;
}


// set dirty bit.
void set_dirty_buffer(struct buffer *buf) {
  buf->is_dirty = 1;
}


void unlock_buffer(struct buffer *buf) {
  pthread_mutex_unlock(&buf->page_latch);
}
