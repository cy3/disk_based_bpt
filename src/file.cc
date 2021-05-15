// file.c
// file layer of DBMS

#include "types.h"
#include "defs.h"
#include "file.h"

int file_open(char *pathname) {
  int fd;
  fd = open(pathname, O_RDWR | O_CREAT | O_SYNC,
      DB_FILE_MOD);
  if (fd < 0) { panic("file_open"); }
  return fd;
}

int file_close(int fd) {
  return close(fd);
}

int file_size(int fd) {
  return lseek(fd, 0, SEEK_END);
}

// extend file by n pages
void file_extend(int fd, pagenum_t cur, int n) {
  page_t temp;
  int i;
  memset(&temp, 0, PAGE_SIZE);
  for (i = 0; i < n; ++i) {
    file_write_page(fd, cur+i, &temp);
  }
}

// Read an on-disk page into in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t *dest) {
  ssize_t sz;
  sz = pread(fd, dest, PAGE_SIZE, pagenum*PAGE_SIZE);
  if (sz != PAGE_SIZE) { panic("file_read_page"); }
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum,
    const page_t *src) {
  ssize_t sz;
  sz = pwrite(fd, src, PAGE_SIZE, pagenum*PAGE_SIZE);
  if (sz != PAGE_SIZE) { panic("file_write_page"); }
}
