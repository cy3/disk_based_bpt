// find.c
// index layer of DBMS

#include "types.h"
#include "defs.h"
#include "page.h"

// Traces the path from root to a leaf.
// Returns the leaf page num may containing the given key.
// if there is no root, return 0
pagenum_t find_leaf(int id, int64_t key) {
  struct ipage *node;
  int i;
  pagenum_t node_num, child_num;

  node_num = get_root_num(id);
  // case : no root
  if (node_num == 0)
    return 0;

  node = get_ipage(id, node_num);
  while (node->header.is_leaf == 0) {
    for (i = 0; i < node->header.num_keys; ++i) {
      if (key < node->index[i].key) { break; }
    }
    if (i == 0) {
      child_num = node->header.leftmost;
    } else {
      child_num = node->index[i-1].pagenum;
    }
    unpin_page(id, node_num);
    node_num = child_num;
    node = get_ipage(id, node_num);
  }
  unpin_page(id, node_num);
  return node_num;
}

// utility function used for finding child index.
// 0 = leftmost, 1 = first key, -1 on fail
int find_index(int id,
    pagenum_t parent_num, pagenum_t child_num) {
  struct ipage *parent = get_ipage(id, parent_num);
  int i;

  if (parent->header.leftmost == child_num) {
    unpin_page(id, parent_num);
    return 0;
  }

  for (i = 0; i < parent->header.num_keys; ++i) {
    if (parent->index[i].pagenum == child_num) { break; }
  }

  if (i == parent->header.num_keys) {
    unpin_page(id, parent_num);
    return -1;
  } else {
    unpin_page(id, parent_num);
    return i+1;
  }
}

// find record in leaf page
// return record's index
// if not found, return nonzero value
int find_record(struct lpage *leaf, int64_t key) {
  int i;
  for (i = 0; i < leaf->header.num_keys; ++i) {
    if (leaf->record[i].key == key) {
      return i;
    }
  }
  return -1;
}


// find
// return nonzero if fail.
// if found, copy value to ret_val.
// if ret_val is NULL, just check existence of key.
int find(int id, int64_t key, char *ret_val) {
  struct lpage *leaf;
  pagenum_t leaf_num;
  int i = 0;

  leaf_num = find_leaf(id, key);
  if (leaf_num == 0) {
    return 1;
  }

  leaf = get_lpage(id, leaf_num);
  if ((i = find_record(leaf, key)) != -1) {
    if(ret_val != NULL)
      memcpy(ret_val, leaf->record[i].value, VALUE_SIZE);
    unpin_page(id, leaf_num);
    return 0;
  }
  unpin_page(id, leaf_num);
  return 1;
}
