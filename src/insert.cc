// insert.c
// index layer of DBMS

#include "types.h"
#include "defs.h"
#include "page.h"

// forward declaration
int insert_parent(int, pagenum_t, int64_t, pagenum_t);

// utility function used for page splitting.
int cut(int length) {
  if (length % 2 == 0)
    return length/2;
  else
    return length/2+1;
}


// creates a new root for two subtrees
// and inserts the appropriate key into the new root
int insert_new_root(int id,
    pagenum_t left_num, int64_t key, pagenum_t right_num) {
  struct ipage *root;
  pagenum_t root_num;

  root_num = alloc_page(id);
  root = get_ipage(id, root_num);
  set_dirty_page(id, root_num);

  // setting new root page
  root->header.parent = 0;
  root->header.num_keys = 1;
  root->header.leftmost = left_num;
  root->index[0].key = key;
  root->index[0].pagenum = right_num;

  unpin_page(id, root_num);
  set_page_parent(id, left_num, root_num);
  set_page_parent(id, right_num, root_num);
  set_root_page(id, root_num);

  return id;
}


// inserts a new key and pointer to a node.
int insert_node(int id,
    pagenum_t node_num, pagenum_t left_num,
    int64_t key, pagenum_t right_num) {
  struct ipage *node;
  int i, left_index;

  // shift indexs in node
  left_index = find_index(id, node_num, left_num);
  node = get_ipage(id, node_num);
  set_dirty_page(id, node_num);

  if (left_index < 0) { panic("insert_node"); }
  for (i = node->header.num_keys; i > left_index; --i)
    node->index[i] = node->index[i-1];
  node->index[left_index].key = key;
  node->index[left_index].pagenum = right_num;
  node->header.num_keys++;

  unpin_page(id, node_num);
  return id;
}


// inserts a new key and pointer to a node.
// node's size exceed the order, split into two.
int insert_node_split(int id,
    pagenum_t node_num, pagenum_t left_num,
    int64_t key, pagenum_t right_num) {
  struct ipage *node;
  struct ipage *new_node;
  pagenum_t new_node_num;
  struct index temp_index[NODE_ORDER];
  int64_t new_key;
  int split = cut(NODE_ORDER);
  int i, j, left_index;

  left_index = find_index(id, node_num, left_num);
  node = get_ipage(id, node_num);
  new_node_num = alloc_page(id);
  new_node = get_ipage(id, new_node_num);
  set_dirty_page(id, node_num);
  set_dirty_page(id, new_node_num);

  // copy indices into temp struct
  for (i = 0, j = 0; i < NODE_ORDER-1; ++i, ++j) {
    if (j == left_index) j++;
    temp_index[j] = node->index[i];
  }
  temp_index[left_index].key = key;
  temp_index[left_index].pagenum = right_num;

  // setting new internal page
  new_key = temp_index[split-1].key;
  new_node->header.leftmost = temp_index[split-1].pagenum;
  new_node->header.parent = node->header.parent;
  new_node->header.is_leaf = 0;
  new_node->header.num_keys = NODE_ORDER - split;
  node->header.num_keys = split - 1;
  set_page_parent(id, new_node->header.leftmost, new_node_num);

  // rewrite temp struct to pages
  for (i = 0; i < split-1; ++i) {
    set_page_parent(id, temp_index[i].pagenum, node_num);
    node->index[i] = temp_index[i];
  }
  for (i = split; i < NODE_ORDER; ++i) {
    set_page_parent(id, temp_index[i].pagenum, new_node_num);
    new_node->index[i - split] = temp_index[i];
  }

  unpin_page(id, node_num);
  unpin_page(id, new_node_num);
  return insert_parent(id, node_num, new_key, new_node_num);
}


// inserts a new node(leaf or internal) into internal page.
// return the id of tree.
int insert_parent(int id,
    pagenum_t left_num, int64_t key, pagenum_t right_num) {
  struct ipage *left;
  struct ipage *parent;
  pagenum_t parent_num;
  int parent_num_keys;

  left = get_ipage(id, left_num);
  parent_num = left->header.parent;
  unpin_page(id, left_num);

  // case : new root
  if (parent_num == 0) {
    return insert_new_root(id, left_num, key, right_num);
  }

  parent = get_ipage(id, parent_num);
  parent_num_keys = parent->header.num_keys;
  unpin_page(id, parent_num);

  if (parent_num_keys < NODE_ORDER - 1) {
    // case : insert key into internal page.
    return insert_node(id,
        parent_num, left_num, key, right_num);
  } else {
    // case : insert and split internal page.
    return insert_node_split(id,
        parent_num, left_num, key, right_num);
  }
}

// insert a record to a leaf.
int insert_leaf(int id, pagenum_t leaf_num,
    struct record *r){
  struct lpage *leaf;
  int i, point;

  leaf = get_lpage(id, leaf_num);
  set_dirty_page(id, leaf_num);

  point = leaf->header.num_keys;
  for (i = 0; i < leaf->header.num_keys; ++i) {
    if (r->key < leaf->record[i].key) { point = i; break; }
  }

  // shift records and insert new record
  for (i = leaf->header.num_keys; i > point; --i)
    leaf->record[i] = leaf->record[i-1];
  leaf->record[i] = *r;
  leaf->header.num_keys++;

  unpin_page(id, leaf_num);
  return id;
}


// inserts a new key and record to a leaf.
// leaf's size exceed the order, split into two.
int insert_leaf_split(int id, pagenum_t leaf_num,
    struct record *r) {
  struct lpage *leaf;
  struct lpage *new_leaf;
  struct record temp_record[LEAF_ORDER];
  pagenum_t new_leaf_num;
  int64_t new_key;
  int split = cut(LEAF_ORDER-1);
  int i, j, r_index;

  leaf = get_lpage(id, leaf_num);
  new_leaf_num = alloc_page(id);
  new_leaf = get_lpage(id, new_leaf_num);
  set_dirty_page(id, leaf_num);
  set_dirty_page(id, new_leaf_num);

  // copy records into temp struct
  for (r_index = 0; r_index < LEAF_ORDER-1; ++r_index) {
    if (leaf->record[r_index].key > r->key) { break; }
  }
  for (i = 0, j = 0; i < LEAF_ORDER-1; ++i, ++j) {
    if (j == r_index) j++;
    temp_record[j] = leaf->record[i];
  }
  temp_record[r_index] = *r;

  // setting new leaf page
  new_leaf->header.sib = leaf->header.sib;
  new_leaf->header.parent = leaf->header.parent;
  new_leaf->header.is_leaf = 1;
  new_leaf->header.num_keys = LEAF_ORDER - split;
  leaf->header.num_keys = split;
  leaf->header.sib = new_leaf_num;
  new_key = temp_record[split].key;

  // insert records at each page
  for (i = 0; i < split; ++i)
    leaf->record[i] = temp_record[i];
  for (i = split; i < LEAF_ORDER; ++i)
    new_leaf->record[i - split] = temp_record[i];

  unpin_page(id, leaf_num);
  unpin_page(id, new_leaf_num);
  return insert_parent(id, leaf_num,
      new_key, new_leaf_num);
}


// master insert function.
// return nonzero if key is duplicated.
int insert(int id, int64_t key, char *value) {
  pagenum_t leaf_num;
  struct lpage *leaf;
  struct record r;
  uint num_keys;

  r.key = key;
  memcpy(r.value, value, VALUE_SIZE);

  if (find(id, r.key, NULL) == 0)
    return 1;

  leaf_num = find_leaf(id, r.key);
  // no root. start new tree.
  if (leaf_num == 0) { 
    leaf_num = start_root_page(id);
  }

  leaf = get_lpage(id, leaf_num);
  num_keys = leaf->header.num_keys;
  unpin_page(id, leaf_num);

  if (num_keys < LEAF_ORDER-1) {
    // leaf has room for key and value.
    insert_leaf(id, leaf_num, &r);
  } else {
    // leaf must be split.
    insert_leaf_split(id, leaf_num, &r);
  }
  return 0;
}

