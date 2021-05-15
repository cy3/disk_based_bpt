// log.cc
// log layer of DBMS

#include "types.h"
#include "defs.h"
#include "page.h"
#include "buffer.h"
#include "log.h"

pthread_mutex_t log_manager_latch = PTHREAD_MUTEX_INITIALIZER;
std::unordered_map<int, uint64_t> trx_lsn_table;
static uint64_t flushed_lsn;
static uint64_t last_lsn;

static FILE *msg_fp;
static FILE *log_fp;


// debug function.
void
print_log(struct log_t &log) {
  printf("size: %d, LSN: %lu, prev_lsn: %lu, trx_id: %d, type: %d",
      log.log_size, log.lsn, log.prev_lsn, log.trx_id, log.type);
  if (log.type == 1 || log.type == 4) {
    printf("\n\ttable_id: %d, pagenum: %lu, offset: %d, len: %d, old: %s, new: %s",
        log.table_id, log.pagenum, log.offset, log.len, log.old_image, log.new_image);
  }
  if (log.type == 4){
    printf(" next_undo_lsn: %lu", log.next_undo_lsn);
  }
  printf("\n");
}


// read one log from offset.
// if end of file or fail, return non zero value.
// Otherwise return 0.
int
log_read(struct log_t &log, uint64_t &log_offset) {
  fseek(log_fp, log_offset, SEEK_SET);
  int log_size;
  //size_t ret = fread(&log_size, sizeof(int), 1, log_fp);
  //if (ret != 1) {
  //  return -1;
  //}
  size_t ret = fread(&log, LOG_HEADER_SIZE, 1, log_fp);
  if (ret != 1) {
    return -1;
  }
  log_size = log.log_size;
  fseek(log_fp, log_offset, SEEK_SET);
  ret = fread(&log, log_size, 1, log_fp);
  if (ret != 1) {
    return -1;
  }
  log_offset += log_size;
  return 0;
}

// if page lsn is higher, return non zero
// mode = 0 : redo, = 1 : undo
int
log_page_write(struct log_t &log, int mode) {
  char file_name[20];
  sprintf(file_name, "DATA%d", log.table_id);
  table_open(file_name);
  struct buffer *buf = get_buffer(log.table_id, log.pagenum);
  struct lpage *leaf = (struct lpage *)(&buf->frame);
  int ret = 0;
  if (leaf->header.page_lsn >= log.lsn) {
    ret = 1;
  } else {
    set_dirty_buffer(buf);
    leaf->header.page_lsn = log.lsn;
    if (mode == 0) {
      memcpy((char *)leaf + log.offset, log.new_image, log.len);
    }
    if (mode == 1) {
      memcpy((char *)leaf + log.offset, log.old_image, log.len);
    }
    //int i = (log.offset - sizeof(struct page_header))/sizeof(struct record);
    //memcpy(leaf->record[i].value, log.new_image, log.len);
  }
  unlock_buffer(buf);
  return ret;
}


// if success, return 0. Otherwise, return non-zero value.
int
log_init(char *log_path, char *logmsg_path, int flag, int log_num) {
  char *buf = (char *)(malloc(sizeof(char) * LOG_BUF_SIZE));
  char *msg_buf = (char *)(malloc(sizeof(char) * LOG_BUF_SIZE));
  struct log_t log;
  uint64_t log_offset = 0;
  log_fp = fopen(log_path, "ab+");
  setvbuf(log_fp, buf, _IOFBF, LOG_BUF_SIZE);
  msg_fp = fopen(logmsg_path, "w");
  setvbuf(msg_fp, msg_buf, _IOFBF, LOG_BUF_SIZE);

  // analysis pass
  fprintf(msg_fp, "[ANALYSIS] Analysis pass start\n");
  std::list<int> winners;
  std::unordered_set<int> losers;
  for (;;) {
    if(log_read(log, log_offset)) { break; }
    trx_lsn_table[log.trx_id] = log.lsn;
    // trx start
    if (log.type == 0) {
      losers.insert(log.trx_id);
    }
    // trx commited or rollbacked
    if (log.type == 2 || log.type == 3) {
      losers.erase(log.trx_id);
      winners.emplace_back(log.trx_id);
      trx_lsn_table.erase(log.trx_id);
    }
  }

  fprintf(msg_fp, "[ANALYSIS] Analysis success. Winner:");
  for (auto i : winners) { fprintf(msg_fp, " %d", i); }
  fprintf(msg_fp, ", Loser:");
  for (auto i : losers) { fprintf(msg_fp, " %d", i); }
  fprintf(msg_fp, "\n");

  // redo pass
  fprintf(msg_fp, "[REDO] Redo pass start\n");
  log_offset = 0;
  for (int i = 0; true; ++i) {
    // KABOOM! CRASH
    if (flag == 1 && i == log_num) {
      close_buffer();
      fclose(log_fp);
      fclose(msg_fp);
      free(buf);
      free(msg_buf);
      return 0;
    }

    if(log_read(log, log_offset)) { break; }
    fprintf(msg_fp, "LSN %lu ", log_offset);
    switch (log.type) {
      case 0: //BEGIN
        fprintf(msg_fp, "[BEGIN] Transaction id %d\n", log.trx_id);
        break;
      case 1: //UPDATE
        if (log_page_write(log, 0)) {
          fprintf(msg_fp, "[CONSIDER-REDO] Transaction id %d\n", log.trx_id);
        } else {
          fprintf(msg_fp, "[UPDATE] Transaction id %d redo apply\n", log.trx_id);
        }
        break;
      case 2: //COMMIT
        fprintf(msg_fp, "[COMMIT] Transaction id %d\n", log.trx_id);
        break;
      case 3: //ROLLBACK
        fprintf(msg_fp, "[ROLLBACK] Transaction id %d\n", log.trx_id);
        break;
      case 4: //CLR
        if (log_page_write(log, 0)) {
          fprintf(msg_fp, "[CONSIDER-REDO] Transaction id %d\n", log.trx_id);
        } else {
          fprintf(msg_fp, "[CLR] next undo lsn %lu\n", log.next_undo_lsn);
        }
        break;
    }
  }
  fprintf(msg_fp, "[REDO] Redo pass end\n");

  // update last lsn
  last_lsn = log_offset;
  flushed_lsn = last_lsn;

  // undo pass
  std::unordered_map<int, uint64_t> active_trx;
  for (auto it = trx_lsn_table.begin(); it != trx_lsn_table.end(); ++it) {
    int cur_trx = it->first;
    active_trx[cur_trx] = trx_lsn_table[cur_trx];
  }
  fprintf(msg_fp, "[UNDO] Undo pass start\n");

  for (int i = 0; true; ++i) {
    // KABOOM! CRASH
    if (flag == 2 && i == log_num) {
      close_buffer();
      fclose(log_fp);
      fclose(msg_fp);
      free(buf);
      free(msg_buf);
      return 0;
    }
    // find nearest LSN
    if (active_trx.size() == 0) { break; }
    int next_trx = active_trx.begin()->first;
    uint64_t next_lsn = active_trx[next_trx];
    for (auto it = active_trx.begin(); it != active_trx.end(); ++it) {
      int cur_trx = it->first;
      if (next_lsn < active_trx[cur_trx]) {
        next_trx = cur_trx;
        next_lsn = active_trx[cur_trx];
      }
    }

    log_read(log, next_lsn);
    uint64_t new_lsn;
    switch(log.type) {
      case 0: //begin
        new_lsn = log_add(log.trx_id, 3);
        fprintf(msg_fp, "LSN %lu ", new_lsn+LOG_HEADER_SIZE);
        fprintf(msg_fp, "[ROLLBACK] Transaction id %d\n", log.trx_id);
        active_trx.erase(log.trx_id);
        break;
      case 1: //update
        new_lsn = log_update(log.trx_id, 4, log.table_id, log.pagenum, log.offset,
            log.len, log.new_image, log.old_image, log.prev_lsn);
        active_trx[log.trx_id] = log.prev_lsn;
        fprintf(msg_fp, "LSN %lu ", new_lsn+LOG_COMPENSATE_SIZE);
        fprintf(msg_fp, "[UPDATE] Transaction id %d undo apply\n", log.trx_id);
        break;
      case 4: //CLR
        active_trx[log.trx_id] = log.next_undo_lsn;
        fprintf(msg_fp, "LSN %lu ", log.lsn+LOG_COMPENSATE_SIZE);
        fprintf(msg_fp, "[CLR] next undo lsn %lu\n", log.next_undo_lsn);
        break;
    }

  }
  fprintf(msg_fp, "[UNDO] Undo pass end\n");

  close_buffer();
  fclose(log_fp);
  fclose(msg_fp);
  free(msg_buf);
  trx_lsn_table.clear();
  log_fp = fopen(log_path, "ab");
  setvbuf(log_fp, buf, _IOFBF, LOG_BUF_SIZE);
  return 0;
}


// flush out all log.
int
log_flush() {
  pthread_mutex_lock(&log_manager_latch);
  fflush(log_fp);
  flushed_lsn = last_lsn;
  pthread_mutex_unlock(&log_manager_latch);
}


uint64_t
log_add(int trx_id, int type) {
  pthread_mutex_lock(&log_manager_latch);
  struct log_t log;
  log.trx_id = trx_id;
  log.log_size = LOG_HEADER_SIZE;
  log.lsn = last_lsn;
  log.type = type;
  if (type == 0) {
    trx_lsn_table[trx_id] = log.lsn;
    log.prev_lsn = 0;
  } else {
    log.prev_lsn = trx_lsn_table[trx_id];
  }
  if (type == 2) {
    trx_lsn_table.erase(trx_id);
  }
  //print_log(log);
  last_lsn += LOG_HEADER_SIZE;
  fseek(log_fp, log.lsn, SEEK_SET);
  fwrite(&log, LOG_HEADER_SIZE, 1, log_fp);
  pthread_mutex_unlock(&log_manager_latch);
  return log.lsn;
}


uint64_t
log_update(int trx_id, int type, int table_id, pagenum_t pagenum,
    int offset, int len, char *old_image, char *new_image, uint64_t next_undo_lsn) {
  pthread_mutex_lock(&log_manager_latch);
  struct log_t log;
  log.trx_id = trx_id;
  log.lsn = last_lsn;
  log.type = type;
  log.prev_lsn = trx_lsn_table[trx_id];
  trx_lsn_table[trx_id] = log.lsn;
  log.table_id = table_id;
  log.pagenum = pagenum;
  log.offset = offset;
  log.len = len;
  memcpy(log.old_image, old_image, len);
  memcpy(log.new_image, new_image, len);
  if (type == 1) {
    last_lsn += LOG_UPDATE_SIZE;
    log.log_size = LOG_UPDATE_SIZE;
    fseek(log_fp, log.lsn, SEEK_SET);
    fwrite(&log, LOG_UPDATE_SIZE, 1, log_fp);
  }
  pthread_mutex_unlock(&log_manager_latch);
  if (type == 4) {
    last_lsn += LOG_COMPENSATE_SIZE;
    log.next_undo_lsn = next_undo_lsn;
    log.log_size = LOG_COMPENSATE_SIZE;
    fseek(log_fp, log.lsn, SEEK_SET);
    fwrite(&log, LOG_COMPENSATE_SIZE, 1, log_fp);
    log_page_write(log, 0);
  }
  return log.lsn;
}


