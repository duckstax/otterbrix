#ifndef otterbrix_otterbrix_H
#define otterbrix_otterbrix_H

#include "cursor_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

///enum level_t { trace, debug, info, warn, err, critical, off, level_nums };

struct config_log_t {
    string_view_t path;
    ////level_t level;
};

struct config_wal_t {
    string_view_t path;
    bool on;
    bool sync_to_disk;
};

struct config_disk_t {
    string_view_t path;
    bool on;
};

typedef struct config_t {
    config_log_t log;
    config_wal_t wal;
    config_disk_t disk;
} config_t;

typedef enum state_t { init, created, destroyed } state_t;

typedef struct result_set_t {
} result_set_t;

typedef void* otterbrix_ptr;
typedef void* cursor_ptr;

otterbrix_ptr otterbrix_create(); // temp
//otterbrix_ptr otterbrix_create(const config_t*); -> should become

void otterbrix_destroy(otterbrix_ptr);

cursor_ptr execute_sql(otterbrix_ptr, string_view_t query);

#ifdef __cplusplus
}
#endif

#endif //otterbrix_otterbrix_H
