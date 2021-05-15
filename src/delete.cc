// delete.c
// index layer of DBMS

#include "types.h"
#include "defs.h"
#include "page.h"

// delete entry from node page.
// node with zero key keeps leftmost child.
// if last leftmost chlid deleted,
// free page and propagate delete to parent node.
int delete_node(int id,
    pagenum_t node_num, pagenum_t child_num) {
  struct ipage *node;
  pagenum_t parent_num, left_num;
  int i, index, num_keys;

  index = find_index(id, node_num, child_num);
  node = get_ipage(id, node_num);
  parent_num = node->header.parent;

  if (node->header.num_keys == 0) { // last leftmost delete
    unpin_page(id, node_num);
    free_page(id, node_num);
    if (parent_num == 0) { // one child root
      set_root_page(id, 0);
      return id;
    } else {
      return delete_node(id, parent_num, node_num);
    }
  }

  set_dirty_page(id, node_num);
  // delete index and shift
  node->header.num_keys--;
  if (index == 0) { // delete leftmost
    node->header.leftmost = node->index[0].pagenum;
    index++;
  }
  for (i = index-1; i < node->header.num_keys; ++i)
    node->index[i] = node->index[i+1];

  unpin_page(id, node_num);
  return id;
}


// find most nearest left leaf in whole tree.
// return 0 if leaf is leaftmost in tree.
pagenum_t find_left_leaf(int id,
    pagenum_t node_num, pagenum_t child_num) {
  struct ipage *node;
  struct ipage *new_node;
  pagenum_t new_node_num;
  int index;

  // find node which have left leaf by upstream.
  for (;;) {
    index = find_index(id, node_num, child_num);
    node = get_ipage(id, node_num);

    if (index == 0) { // leftmost. open parent
      new_node_num = node->header.parent;
      unpin_page(id, node_num);
      if (new_node_num == 0) { // leaf is leftmost in root.
        return 0;
      }
      child_num = node_num;
      node_num = new_node_num;
    } else { // found left node.
      if (index == 1) {
        new_node_num = node->header.leftmost;
      } else {
        new_node_num = node->index[index-2].pagenum;
      }
      unpin_page(id, node_num);
      break;
    }
  }

  // new find rightmost leaf by downstream.
  new_node = get_ipage(id, new_node_num);
  while (new_node->header.is_leaf == 0) {
    index = new_node->header.num_keys;
    if (index == 0) {
      node_num = new_node->header.leftmost;
    } else {
      node_num = new_node->index[index-1].pagenum;
    }
    unpin_page(id, new_node_num);
    new_node_num = node_num;
    new_node = get_ipage(id, new_node_num);
  }
  unpin_page(id, new_node_num);

  return new_node_num;
}


// before delete leaf page in node,
// to preserve leaf's sibling structure,
// find left leaf and copy leaf's sibling.
// then free leaf page and delete leaf in node.
int delete_leaf_free(int id,
    pagenum_t node_num, pagenum_t leaf_num) {
  struct ipage *node;
  struct lpage *leaf, *left;
  pagenum_t left_num, sib;
  int index;

  // save sib and delete leaf from node
  left_num = find_left_leaf(id, node_num, leaf_num);
  leaf = get_lpage(id, leaf_num);
  sib = leaf->header.sib;
  unpin_page(id, leaf_num);
  free_page(id, leaf_num);

  // copy sibling to left leaf
  if (left_num != 0) {
    left = get_lpage(id, left_num);
    set_dirty_page(id, left_num);
    left->header.sib = sib;
    unpin_page(id, left_num);
  }

  return delete_node(id, node_num, leaf_num);
}


// delete entry from leaf page.
// if page has only one entry,
// call delete_leaf_free() to free page.
int delete_leaf(int id,
    pagenum_t leaf_num, int64_t key) {
  struct lpage *leaf;
  pagenum_t parent_num;
  int i, index;

  leaf = get_lpage(id, leaf_num);
  set_dirty_page(id, leaf_num);

  // last record delete
  if (leaf->header.num_keys == 1) {
    parent_num = leaf->header.parent;
    unpin_page(id, leaf_num);

    if (parent_num == 0) {
      // if root leaf has one entry, delete root.
      free_page(id, leaf_num);
      set_root_page(id, 0);
    } else {
      // delete leaf
      delete_leaf_free(id, parent_num, leaf_num);
    }
      return id;
  }

  // shift records
  index = find_record(leaf, key);
  leaf->header.num_keys--;
  for (i = index; i < leaf->header.num_keys; ++i)
    leaf->record[i] = leaf->record[i+1];

  unpin_page(id, leaf_num);
  return id;
}

// delete
// return nonzero if fail
int delete_low(int id, int64_t key) {
  pagenum_t leaf_num;

  if (find(id, key, NULL)) {
    return 1; // not exist
  }
  if ((leaf_num = find_leaf(id, key)) == 0) {
    return 1;
  }
  delete_leaf(id, leaf_num, key);
  return 0;
}
