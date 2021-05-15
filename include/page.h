// page.h
// describe detailed page sturcture
#include "types.h"
#define BULK_ALLOC_NUM (10)
#define VALUE_SIZE (120)
#define NODE_ORDER (249)
#define LEAF_ORDER (32)
#define HPAGE_NUM (0)

// page header
struct page_header {
    pagenum_t parent; // parent page number
    uint is_leaf; // 0 = internal, 1 = leaf
    uint num_keys; // number of keys in this page
    int64_t reserved;
    int64_t page_lsn; // recent updated lsn
    uint8_t pad[88];
    union {
        pagenum_t leftmost; // leftmost pagenum in internal
        pagenum_t sib; // right sibling pagenum in leaf
    };
};

// record struct
struct record {
    int64_t key;
    char value[VALUE_SIZE];
};

// internal node index struct
struct index {
    int64_t key;
    pagenum_t pagenum;
};

// header page
struct hpage {
    pagenum_t free; //free page number
    pagenum_t root; //root page number
    pagenum_t numpage; //num of pages
    uint8_t pad[4072];
};

// free page
struct fpage {
    pagenum_t next; //next free page number
    uint8_t pad[4088];
};

// leaf page
struct lpage {
    struct page_header header;
    struct record record[LEAF_ORDER-1];
};

// internal page
struct ipage {
    struct page_header header;
    struct index index[NODE_ORDER-1];
};
