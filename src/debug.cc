// debug.c
// error handing and check db infomation.

#include "types.h"
#include "defs.h"
#include "page.h"

void panic(const char *str) {
  fprintf(stderr, "panic: %s\n", str);
  exit(1);
}

// return number of free pages in db.
int get_free_count(int id, pagenum_t free_num) {
  struct fpage *fp;
  pagenum_t next;
  int i = 1;
  if (free_num == 0) { return 0; }
  fp = get_fpage(id, free_num);
  while (fp->next != 0) {
    next = fp->next;
    i++;
    unpin_page(id, free_num);
    free_num = next;
    fp = get_fpage(id, free_num);
  }
  unpin_page(id, free_num);
  return i;
}

// return height of b+ tree in db.
int get_depth(int id, pagenum_t root_num) {
  struct ipage *node;
  pagenum_t next;
  int i = 1;

  if (root_num == 0)
    return 0;

  node = get_ipage(id, root_num);
  while(node->header.is_leaf == 0) {
    i++;
    next = node->header.leftmost;
    unpin_page(id, root_num);
    root_num = next;
    node = get_ipage(id, root_num);
  }
  unpin_page(id, root_num);
  return i;
}

// return leftmost leaf page number in db.
pagenum_t get_first_leaf(int id, pagenum_t root_num) {
  struct ipage *node;
  pagenum_t next = root_num;
  pagenum_t temp;

  node = get_ipage(id, next);
  while(node->header.is_leaf == 0) {
    temp = node->header.leftmost;
    unpin_page(id, next);
    next = temp;
    node = get_ipage(id, next);
  }
  unpin_page(id, next);
  return next;
}

// return number of leaf pages in db.
int get_leaf_count(int id, pagenum_t root_num) {
  struct lpage *leaf;
  pagenum_t leaf_num, next;
  int i = 1;

  if (root_num == 0)
    return 0;

  leaf_num = get_first_leaf(id, root_num);
  leaf = get_lpage(id, leaf_num);
  while (leaf->header.sib != 0) {
    next = leaf->header.sib;
    i++;
    unpin_page(id, leaf_num);
    leaf_num = next;
    leaf = get_lpage(id, leaf_num);
  }
  leaf = get_lpage(id, leaf_num);
  return i;
}

// print db related informations.
void db_info(int id) {
  struct hpage *head;
  head = get_hpage(id);
  printf(
      "== DB INFO (%d) ==\n"
      "root : %d, leafs: %d\n"
      "free : %d, frees: %d\n"
      "num  : %d, depth: %d\n"
      "==================\n", id,
      (int)head->root, get_leaf_count(id, head->root),
      (int)head->free, get_free_count(id, head->free),
      (int)head->numpage, get_depth(id, head->root));
  unpin_page(id, HPAGE_NUM);
}
