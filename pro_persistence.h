#include <time.h>

void pst_init(const char *mode);
void pst_close(void);
size_t pst_bucket_ing(const item * it, char * bucket_buffer);
void pst_bucket_end(char * bucket_buffer, size_t len);
void pst_warmup(void);

int start_pst_thread(void);
void stop_pst_thread(void);
void enable_pst_thread(void);
void disable_pst_thread(void);

/* structure for gather stat of persistence */
struct pst_stat_type {
  unsigned long long pst_num;     // persistence item number
  unsigned long long expired_num; // expired item found
  unsigned long long flushed_num; // flushed item found
  time_t cur_init;                // start time of current persistence
  time_t cur_close;               // end time of current persistence
  int psting;                     // whether persistence is doing
  int pst_thread;       // whether independ persistence thread is running
  int pst_enable;       // whether independ persistence thread is enabled
  int wu_running;       // whether warmup is being done
  unsigned long long wu_num;      // item number loaded
  int pst_last;                   // last persistence file index
};

extern struct pst_stat_type pst_stat;
extern pthread_cond_t pst_enable_cond;
extern item** primary_hashtable;
extern char *default_path;
extern int pst_interval;
