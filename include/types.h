#include <inttypes.h>

#ifndef _bpt_types_h_
#define _bpt_types_h_

typedef unsigned int uint;
typedef uint64_t pagenum_t;

#define PAGE_SIZE (4096)
// in-memory page structure
typedef struct page_t {
    uint8_t pad[PAGE_SIZE];
} page_t;

#endif
