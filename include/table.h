// table.h
#include <cstdlib>
#define MAX_PATHNAME (30)
#define MAX_TABLE (10)

struct table {
  char pathname[MAX_PATHNAME];
  int id;
  int fd;
};
