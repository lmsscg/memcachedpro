#include "memcached.h"
#include <stdio.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>

#define PST_MAX_PATH_LEN 1024
static int pst_file_number = 2;  // max history persistence file
char *default_path = "/tmp/dump_file";  // default persistence path
static char *pst_postfix = "pst_last";  // file to save last persistence file index
static int pst_file_index = 0;  // current persistence file index
static int pst_fd = -1;  // persistence file discriptor
struct pst_stat_type pst_stat;  // status of persistence
static bool psting = false;  // whether persistence is being done
static size_t pst_fsync_len = 0;  // data size in persistence file without sync
static size_t pst_fsync_max_len = 1000 * 1000 * 50; /* do sync when pst_fsync_len is greater than */

/* copy from assoc.c */
typedef  unsigned long  int  ub4;
#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

static pthread_t pst_tid;  // independ persistence thread id
static volatile int do_run_pst_thread = 1;  // stop independ persistence thread
static volatile int pst_enable = 1;  // pause or resume the independ persistence thread by assoc_maintenance_thread
static unsigned int pst_bucket = 0;  // bucket id being persisted
int pst_interval = 10;  // default persistence interval

/* Macro to write specific length of data */
#define PST_WRITE(fd, buffer, len, ret) {\
  off_t offset = 0;\
  do {\
    ret = write(fd, (void*)&buffer[offset], len - offset);\
    if (ret < 0) {\
      if (errno != EINTR) {\
        fprintf(stderr, "Failed to write: %s\n", strerror(errno));\
        break;\
      }\
    } else {\
      offset += ret;\
    }\
  } while (offset < len);\
}

/* Macro to read specific length of data */
#define PST_READ(fd, buffer, len, ret) {\
  off_t offset = 0;\
  while (len != 0 && (ret = read(fd, (void*)&buffer[offset],\
                                 len)) != 0){\
    if (ret == -1) {\
      if (errno == EINTR)\
        continue;\
      fprintf(stderr, "Failed to read : %s\n", strerror(errno));\
      break;\
    }\
    len -= ret;\
    offset += ret;\
  }\
}

/* recursively make path */
static int pst_mkpath(const char * path) {
  assert(path);
  struct stat fs;
  int ret = stat(path, &fs);
  if (ret == 0) {
    if (S_ISDIR(fs.st_mode)) {
      return 0;
    }
  } else if (ENOENT != errno) {  // other error than non-exist
    fprintf(stderr, "[persistence] stat error : %s\n", strerror(errno));
    return -1;
  }

  char tmp_path[PST_MAX_PATH_LEN];
  strncpy(tmp_path, path, PST_MAX_PATH_LEN);
  tmp_path[PST_MAX_PATH_LEN - 1] = '\0';

  char sub_path[PST_MAX_PATH_LEN] = "";  // partial path to be created
  char * saveptr = NULL;
  char * ptr = strtok_r(tmp_path, "/", &saveptr);

  while (ptr) {  // has more path component
    if (settings.verbose >= 1) {
      fprintf(stderr, "[persistence] path component : %s\n", ptr);
    }

    size_t sub_len = strlen(sub_path);
    /* cat the current component after sub_path */
    if ((PST_MAX_PATH_LEN - sub_len < 1) || (snprintf(&sub_path[sub_len],
                          PST_MAX_PATH_LEN - sub_len, "/%s", ptr) < 0)) {
      fprintf(stderr, "[persistence] failed to print sub path\n");
      return -1;
    }

    /* check sub_path status */
    if (stat(sub_path, &fs) < 0) {
      if (errno != ENOENT) {
        fprintf(stderr, "[persistence] failed to stat sub path : %s\n",
                strerror(errno));
        return -3;
      }

      /* path not exist, creat it */
      if (mkdir(sub_path, 0755) < 0) {
        fprintf(stderr, "[persistence] failed to mkdir (%s) : %s\n",
                sub_path, strerror(errno));
        return -2;
      }
    }
    ptr = strtok_r(NULL, "/", &saveptr);
  }
  return 0;
}

/* initiate the persistence file
   mode : "embed" - embeded in assoc maintenance thread
          "independ" - independed persistence thread
*/
void pst_init(const char * mode) {
  assert(mode);
  if (settings.verbose >= 1) {
    fprintf(stderr, "[persistence] persistence started : %s\n", mode);
  }

  /* init the status structure */
  pst_stat.psting = 1;
  pst_stat.cur_init = time(NULL);
  pst_stat.cur_close = 0;
  pst_stat.pst_num = -1;
  pst_stat.expired_num = 0;
  pst_stat.flushed_num = 0;

  /* construct the persistence file path */
  char pst_file[PST_MAX_PATH_LEN];
  if (snprintf(pst_file, PST_MAX_PATH_LEN, "%s/%d", settings.pst_path,
               pst_file_index) < 0) {
    fprintf(stderr, "[persistence] failed to get pst file.\n");
    return;
  }

  if (settings.verbose >= 2) {
    fprintf(stderr, "[persistence] pst_file = %s\n", pst_file); 
    fprintf(stderr, "[persistence] pst path : %s\n", settings.pst_path);
  }

  /* make the directory */
  if ((strlen(pst_file) > 0) && (!pst_mkpath(settings.pst_path))) {
    pst_fd = open(pst_file, O_WRONLY | O_APPEND |O_CREAT | O_TRUNC, 0644);
    if (pst_fd < 0) {
      fprintf(stderr, "[persistence] failed to open %s : %s\n", pst_file,
              strerror(errno));
    }
  }
  psting = true;
}

/* copy item to the bucket buffer */
size_t pst_bucket_ing(const item *it, char *bucket_buffer) {
  if (pst_fd < 0 || !it || !bucket_buffer) {
    return 0;
  }

  if (settings.oldest_live != 0 && settings.oldest_live <= current_time &&
      it->time <= settings.oldest_live) {
    /* item is flushed, ignoring it */
    pst_stat.flushed_num++;
    return 0;
  } else if (it->exptime != 0 && it->exptime <= current_time) {
    /* item is expired, ignoring it */
    pst_stat.expired_num++;
    return 0;
  }
  pst_stat.pst_num++;

  /* copy the length, then the structure */
  size_t len = ITEM_ntotal(it);
  memcpy(bucket_buffer, (char *)&len, sizeof(len));
  memcpy(bucket_buffer + sizeof(len), (const char *)it, len);
  return len + sizeof(len);
}

/* write an item into the persistence file */
static void pst_bucket_item(const item * it) {
  size_t len = ITEM_ntotal(it);
  size_t lenoflen = sizeof(len);
  ssize_t ret;
  char *buffer = (char *)&len;

  /* write the length */
  PST_WRITE(pst_fd, buffer, lenoflen, ret);
  if (ret < 0) {
    return;
  }

  /* write the structure */
  buffer = (char *)it;
  PST_WRITE(pst_fd, buffer, len, ret);
  if (ret < 0) {
    return;
  }
}

/* write the bucket buffer into persistence file */
void pst_bucket_end(char * bucket_buffer, size_t len) {
  if ((pst_fd < 0) || (bucket_buffer == NULL)) {
    return;
  }
  
  ssize_t ret;
  PST_WRITE(pst_fd, bucket_buffer, len, ret);
  if (ret < 0) {
    return;
  }

  /* fsync it, when greater than pst_fsync_len_max */
  pst_fsync_len += len;
  if (pst_fsync_len > pst_fsync_max_len) {
    fsync(pst_fd);
    pst_fsync_len = 0;
  }
}

/* close the persistence file */
void pst_close(void) {
  if (pst_fd < 0) {
    return;
  }

  /* write a null item to tell the itegrity of persistence */
  item eof;
  eof.nkey = 0;
  eof.nbytes = 0;
  eof.nsuffix = 0;
  pst_bucket_item(&eof);

  if (pst_fd > 0) {
    close(pst_fd);
    pst_fd = -1;
  }

  psting = false;
  pst_stat.cur_close = time(NULL);
  pst_stat.psting = 0;

  /* save the last successful persistence file index in the last file */
  char last_file[PST_MAX_PATH_LEN];
  if (snprintf(last_file, PST_MAX_PATH_LEN, "%s/%s", settings.pst_path,
               pst_postfix) < 0) {
    fprintf(stderr, "[persistence] failed to get last file.\n");
    return;
  }
  int last_fd = open(last_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (last_fd < 0) {
    fprintf(stderr, "[persistence] failed to open last file : %s\n",
            strerror(errno));
    return;
  }

  char index_str[PST_MAX_PATH_LEN];
  if (snprintf(index_str, PST_MAX_PATH_LEN, "%d", pst_file_index) < 0) {
    fprintf(stderr, "[persistence] failed to print pst_file_index\n");
    return;
  }

  size_t len = strlen(index_str);
  ssize_t ret;
  char *buffer = (char *)&index_str[0];
  PST_WRITE(last_fd, buffer, len, ret);
  if ((ret >= 0) && !close(last_fd)) {
    pst_stat.pst_last = pst_file_index;
    pst_file_index = (pst_file_index + 1) % pst_file_number;
    if (settings.verbose >= 1) {
      fprintf(stderr, "[persistence] persistence ended\n");
    }
  } else if (ret < 0) {
    close(last_fd);
    if (settings.verbose >= 1) {
      fprintf(stderr, "[persistence] failed to save index\n");
    }
  }
}

/* abort the independ persistence thread, when assoc maintenace thread disable it */
static void pst_abort(void) {
  if (pst_fd > 0) {
    close(pst_fd);
    pst_fd = -1;
  }

  psting = false;
  pst_stat.cur_close = time(NULL);
  pst_stat.psting = 0;
  if (settings.verbose >= 1) {
    fprintf(stderr, "[persistence] persistence aborted\n");
  }
}

/* initiate the warmup file */
static int pst_warmup_init(void) {
  if (settings.verbose >= 1) {
    fprintf(stderr, "[persistence] warmup started\n");
    fprintf(stderr, "[persistence] warmup path : %s\n",
            settings.warmup_path);
  }
  pst_stat.wu_running = 1;
  pst_stat.wu_num = 0;
  pst_stat.pst_last = -1;

  /* read persistence file index from last file */
  char last_file[PST_MAX_PATH_LEN];
  if (snprintf(last_file, PST_MAX_PATH_LEN, "%s/%s", settings.warmup_path,
               pst_postfix) < 0) {
    fprintf(stderr, "[persistence] failed to get last file.\n");
    return -1;
  }
  int last_fd = open(last_file, O_RDONLY);
  if (last_fd < 0) {
    fprintf(stderr, "[persistence] failed to open last file : %s\n",
            strerror(errno));
    return -1;
  }
  char pst_file[PST_MAX_PATH_LEN];
  if (read(last_fd, pst_file, PST_MAX_PATH_LEN) < 0) {
    fprintf(stderr, "[persistence] failed to read last file : %s\n",
            strerror(errno));
    close(last_fd);
    return -1;
  }
  close(last_fd);

  /* initiate the warmup file, according to the persistence file index */
  int pst_last;
  if (sscanf(pst_file, "%d", &pst_last) != 1) {
    fprintf(stderr, "[persistence] failed to get pst last.\n");
    return -1;
  }
  pst_stat.pst_last = pst_last;
  pst_file_index = (pst_last + 1) % pst_file_number;

  if (snprintf(pst_file, PST_MAX_PATH_LEN, "%s/%d", settings.warmup_path,
               pst_last) < 0) {
    fprintf(stderr, "[persistence] failed to get pst file.\n");
    return -1;
  }

  int pst_fd = open(pst_file, O_RDONLY);
  if (pst_fd < 0) {
    fprintf(stderr, "[persistence] failed to open pst file : %s\n",
            strerror(errno));
    return -1;
  }
  return pst_fd;
}

/* load the items from warmup file */
void pst_warmup(void) {
  int pst_fd = pst_warmup_init();
  if (pst_fd < 0) {
    fprintf(stderr, "[persistence] warmup aborted\n");
    return;
  }

  item *it = NULL;
  size_t len;
  size_t lenoflen;
  ssize_t ret;
  char *buffer = NULL;
  do {
    /* read the length of item */
    buffer = (char *)&len;
    lenoflen = sizeof(len);
    PST_READ(pst_fd, buffer, lenoflen, ret);
    if (ret < 0) {
      break;
    }

    it = (item *)realloc(it, len);
    if (!buffer) {
      fprintf(stderr, "[persistence] failed to malloc");
      break;
    }

    /* read the item structure */
    buffer = (char *)it;
    PST_READ(pst_fd, buffer, len, ret);
    if (ret < 0) {
      break;
    }

    if (it->nkey == 0) {
      /* get the null item at the end of warmup file */
      if (settings.verbose >= 1) {
        fprintf(stderr, "[persistence] get null item\n");
      }
      break;
    }

    /* store the loaded item into hash table */
    pst_store_item(it);
    pst_stat.wu_num++;
  } while (1);

  close(pst_fd);
  if (buffer) {
    free(buffer);
  }

  pst_stat.wu_running = 0;
  if (settings.verbose >= 1) {
    fprintf(stderr, "[persistence] warmup ended\n");
  }
}

/* the independ persistence thread */
static void * pst_thread(void *arg) {
  if (settings.verbose >= 1) {
    fprintf(stderr, "[persistence] persistence thread started\n");
  }

  pst_bucket = 0;
  while (do_run_pst_thread) {
    pst_stat.pst_thread = 1;
    if (!pst_enable) {
      /* the independ persistence thread is diabled by assoc maintanence thread */
      pst_abort();
    } else {
      struct timeval stv;
      gettimeofday(&stv, NULL);
      int bn = 0;  // item number in bucket
      size_t bz = 0;  // bucket buffer size

      item_lock_global();
      mutex_lock(&cache_lock);

      // caculate the size of bucket_buffer
      item * it = primary_hashtable[pst_bucket];
      while (it) {
        /* we save the length of item ahead of the item structure*/
        bz += ITEM_ntotal(it) + sizeof(size_t);
        it = it->h_next;
        bn++;
      }
      /* the buffer to hold the items in a bucket */
      char bucket_buffer[bz];

      // scan the bucket
      size_t bo = 0;
      it = primary_hashtable[pst_bucket];
      while (it) {
        // copy the item into bucket_buffer
        bo += pst_bucket_ing(it, &bucket_buffer[bo]);
        it = it->h_next;
      }
      pst_bucket++;
      mutex_unlock(&cache_lock);
      item_unlock_global();

      struct timeval etv;
      gettimeofday(&etv, NULL);
      if (settings.verbose >= 3) {
        fprintf(stderr, "[persistence] scan bucket[%d] time: %ld:%ld\n",
          pst_bucket, etv.tv_sec - stv.tv_sec, etv.tv_usec - stv.tv_usec);
      }

      // write the bucket_buffer to persistence file
      pst_bucket_end(bucket_buffer, bo);

      // close the persistence file 
      if (pst_bucket == hashsize(hashpower)) {
        pst_close();
      } 
    }

    if (!psting) {
      switch_item_lock_type(ITEM_LOCK_GRANULAR);
      slabs_rebalancer_resume();

      int i = 0;
      pst_stat.pst_thread = 0;
      // when it is diabled or still within interval
      while (do_run_pst_thread && (!pst_enable ||
             i < settings.pst_interval)) {
        sleep(1);
        if (!pst_enable) {  // is disabled
          i = 0;
        } else {  // within interval
          i++;
        }
      }
      if (!do_run_pst_thread) {  // is stopped
        break;
      }

      if (do_run_pst_thread) {
        slabs_rebalancer_pause();
        switch_item_lock_type(ITEM_LOCK_GLOBAL);
        pst_bucket = 0;
        // initiate the persistence file
        pst_init("independ");
      }
    }
  }
  return NULL;
}

// start the independ persistence thread
int start_pst_thread(void) {
  int ret;
  if ((ret = pthread_create(&pst_tid, NULL, pst_thread, NULL)) != 0) {
    fprintf(stderr, "[persistence] Can't create persistence thread: %s\n",
            strerror(ret));
    return -1;
  }
  return 0;
}

// stop the independ persistence thread
void stop_pst_thread(void) {
  do_run_pst_thread = 0;

  pthread_join(pst_tid, NULL);
  if (settings.verbose >= 1) {
    fprintf(stderr, "[persistence] persistence thread stopped\n");
  }
}

// pause the independ persistence thread
void enable_pst_thread(void) {
  pst_enable = 1;
  pst_stat.pst_enable = 1;
}

// resume the independ persistence thread
void disable_pst_thread(void) {
  pst_stat.pst_enable = 0;
  pst_enable = 0;
}
