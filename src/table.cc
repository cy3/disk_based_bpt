// table.c
// buffer layer of DBMS

#include "types.h"
#include "defs.h"
#include "table.h"

static struct table tables[MAX_TABLE];

// find table's fd
// if found fail, return 0.
int find_fd(int id) {
  int i;
  for (i = 0; i < MAX_TABLE; ++i) {
    if (tables[i].id == id) { return tables[i].fd; }
  }
  return 0;
}


// if file is open, return 0.
int check_table(int id) {
  int fd;
  fd = find_fd(id);
  if (fd) { return 0; } else { return 1; }
}


// open table.
int table_open(char *pathname) {
  int i;
  int path_id;
  //int fd, sz;

  if (pathname == NULL) { return -1; }
  if (strlen(pathname) >= MAX_PATHNAME) { return -1; }

  // path should be DATA(NUM) format.
  if (strlen(pathname) < 5) { return -1; }
  path_id = std::atoi(pathname+4);

  // find from list
  for (i = 0; i < MAX_TABLE; ++i) {
    if (strcmp(tables[i].pathname, pathname) == 0) {
      if (tables[i].fd == 0) {
        tables[i].fd = file_open(pathname);
      }
      return tables[i].id;
    }
  }

  // or add one to list
  for (i = 0; i < MAX_TABLE; ++i) {
    if (tables[i].id == 0) {
      strcpy(tables[i].pathname, pathname);
      tables[i].fd = file_open(pathname);
      tables[i].id = path_id;
      if (file_size(tables[i].fd) == 0)
        init_head_page(tables[i].id);
      return tables[i].id;
    }
  }

  // table add fail: full table
  return -1;
}


int table_close(int id) {
  int fd;
  fd = find_fd(id);
  if (fd < 1) { return -1; }
  flush_table(id);
  file_close(fd);
  tables[id-1].fd = 0;
  return 0;
}


void close_all_table() {
  int i;
  close_buffer();
  for (i = 0; i < MAX_TABLE; ++i) {
    if (tables[i].fd != 0) {
      file_close(tables[i].fd);
      tables[i].fd = 0;
    }
  }
}
