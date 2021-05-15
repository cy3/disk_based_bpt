//file.h
#include <fcntl.h> //file open
#include <unistd.h> //fild read, write, close
#define DB_FILE_MOD (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
