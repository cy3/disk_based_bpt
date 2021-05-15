// page.c
// page-related functions used by index layer.

#include "types.h"
#include "defs.h"
#include "page.h"

struct hpage *get_hpage(int id) {
  return (struct hpage *)get_page(id, HPAGE_NUM);
}

struct fpage *get_fpage(int id, pagenum_t num) {
  return (struct fpage *)get_page(id, num);
}

struct lpage *get_lpage(int id, pagenum_t num) {
  return (struct lpage *)get_page(id, num);
}

struct ipage *get_ipage(int id, pagenum_t num) {
  return (struct ipage *)get_page(id, num);
}


// create header page.
void init_head_page(int id) {
  struct hpage *head;

  file_extend(find_fd(id), HPAGE_NUM, 1);
  head = get_hpage(id);
  set_dirty_page(id, HPAGE_NUM);
  head->free = 0;
  head->root = 0;
  head->numpage = 1;
  unpin_page(id, HPAGE_NUM);
}


pagenum_t get_root_num(int id) {
  struct hpage *head;
  pagenum_t r;

  head = get_hpage(id);
  r = head->root;
  unpin_page(id, HPAGE_NUM);
  return r;
}


// set new root in header.
int set_root_page(int id, pagenum_t root_num) {
  struct hpage *head;

  head = get_hpage(id);
  set_dirty_page(id, HPAGE_NUM);
  head->root = root_num;
  unpin_page(id, HPAGE_NUM);
  return 0;
}


// start new tree.
pagenum_t start_root_page(int id) {
  struct lpage *leaf;
  pagenum_t leaf_num;

  leaf_num = alloc_page(id);
  leaf = get_lpage(id, leaf_num);
  set_dirty_page(id, leaf_num);
  leaf->header.is_leaf = 1;
  unpin_page(id, leaf_num);
  set_root_page(id, leaf_num);
  return leaf_num;
}


// set child's parent to given parent_num.
int set_page_parent(int id,
    pagenum_t child_num, pagenum_t parent_num) {
  struct ipage *child;
 
  child = get_ipage(id, child_num);
  set_dirty_page(id, child_num);
  child->header.parent = parent_num;
  unpin_page(id, child_num);
  return 0;
}


// create free pages
// return one buffer page num.
pagenum_t create_page(int id) {
  struct hpage *hpage;
  struct fpage *fpage;
  pagenum_t free_num;
  int i;

  hpage = get_hpage(id);
  set_dirty_page(id, HPAGE_NUM);

  free_num = hpage->numpage;
  file_extend(find_fd(id), free_num, BULK_ALLOC_NUM);
  hpage->numpage += BULK_ALLOC_NUM;
  if (BULK_ALLOC_NUM > 1)
    hpage->free = free_num;
  unpin_page(id, HPAGE_NUM);

  for (i = 1; i <= BULK_ALLOC_NUM; ++i) {
    fpage = get_fpage(id, free_num);
    set_dirty_page(id, free_num);
    if (i <= BULK_ALLOC_NUM-2) {
      fpage->next = free_num+1;
    }
    unpin_page(id,free_num);
    free_num++;
  }
  return free_num-1;
}


// alloc on-disk page from free page list.
pagenum_t alloc_page(int id) {
  struct hpage *hpage;
  struct fpage *fpage;
  pagenum_t free_num;

  hpage = get_hpage(id);
  free_num = hpage->free;

  //check available free page
  if (free_num == 0) {
    // no free page : create new page
    unpin_page(id, HPAGE_NUM);
    return create_page(id);
  }

  // return first free page
  fpage = get_fpage(id, free_num);
  set_dirty_page(id, free_num);
  set_dirty_page(id, HPAGE_NUM);
  hpage->free = fpage->next;
  memset(fpage, 0, PAGE_SIZE);
  unpin_page(id, free_num);
  unpin_page(id, HPAGE_NUM);

  return free_num;
}


// delete page from file.
// Free an on-disk page to the free page list.
void free_page(int id, pagenum_t num) {
  struct hpage *hpage;
  struct fpage *fpage;

  hpage = get_hpage(id);
  fpage = get_fpage(id, num);

  set_dirty_page(id, HPAGE_NUM);
  set_dirty_page(id, num);

  fpage->next = hpage->free;
  hpage->free = num;

  unpin_page(id, HPAGE_NUM);
  unpin_page(id, num);
}
