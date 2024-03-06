#ifndef otterbrix_otterbrix_H
#define otterbrix_otterbrix_H

#include <components/cursor/cursor.hpp>
#include <cstdint>
#include <cstdlib>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct string_view_t {
    const char* data;
    size_t size;
} string_view_t;

typedef struct config_t {
    int level;
    string_view_t log_path;
    string_view_t wal_path;
    string_view_t disk_path;
    bool wal_on;
    bool disk_on;
    bool sync_to_disk;
} config_t;

typedef enum state_t { init, created, destroyed } state_t;

typedef void* otterbrix_ptr;
typedef void* cursor_ptr;
typedef void* doc_ptr;

typedef struct error_message {
    int32_t code;
    char* message;
} error_message;

otterbrix_ptr otterbrix_create(config_t cfg, string_view_t database, string_view_t collection);

void otterbrix_destroy(otterbrix_ptr);

cursor_ptr execute_sql(otterbrix_ptr, string_view_t query);

void release_cursor(cursor_ptr ptr);

int32_t cursor_size(cursor_ptr ptr);

bool cursor_has_next(cursor_ptr ptr);

doc_ptr cursor_next(cursor_ptr ptr);

doc_ptr cursor_get(cursor_ptr ptr);

doc_ptr cursor_get_by_index(cursor_ptr ptr, int index);

bool cursor_Is_success(cursor_ptr ptr);

bool cursor_is_error(cursor_ptr ptr);

error_message cursor_get_error(cursor_ptr ptr);

void release_document(doc_ptr ptr);

char* document_id(doc_ptr ptr);

bool document_is_valid(doc_ptr ptr);

bool document_is_array(doc_ptr ptr);

bool document_is_dict(doc_ptr ptr);

int32_t document_count(doc_ptr ptr);

bool document_is_exist_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_exist_by_index(doc_ptr ptr, int32_t index);

bool document_is_null_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_null_by_index(doc_ptr ptr, int32_t index);

bool document_is_bool_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_bool_by_index(doc_ptr ptr, int32_t index);

bool document_is_ulong_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_ulong_by_index(doc_ptr ptr, int32_t index);

bool document_is_long_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_long_by_index(doc_ptr ptr, int32_t index);

bool document_is_double_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_double_by_index(doc_ptr ptr, int32_t index);

bool document_is_string_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_string_by_index(doc_ptr ptr, int32_t index);

bool document_is_array_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_array_by_index(doc_ptr ptr, int32_t index);

bool document_is_dict_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_is_dict_by_index(doc_ptr ptr, int32_t index);

bool document_get_bool_by_key(doc_ptr ptr, string_view_t key_raw);

bool document_get_bool_by_index(doc_ptr ptr, int32_t index);

uint64_t document_get_ulong_by_key(doc_ptr ptr, string_view_t key_raw);

uint64_t document_get_ulong_by_index(doc_ptr ptr, int32_t index);

int64_t document_get_long_by_key(doc_ptr ptr, string_view_t key_raw);

int64_t document_get_long_by_index(doc_ptr ptr, int32_t index);

double document_get_double_by_key(doc_ptr ptr, string_view_t key_raw);

double document_get_double_by_index(doc_ptr ptr, int32_t index);

char* document_get_string_by_key(doc_ptr ptr, string_view_t key_raw);

char* document_get_string_by_index(doc_ptr ptr, int32_t index);

doc_ptr document_get_array_by_key(doc_ptr ptr, string_view_t key_raw);

doc_ptr document_get_array_by_index(doc_ptr ptr, int32_t index);

doc_ptr document_get_dict_by_key(doc_ptr ptr, string_view_t key_raw);

doc_ptr document_get_dict_by_index(doc_ptr ptr, int32_t index);

#ifdef __cplusplus
}
#endif

#endif //otterbrix_otterbrix_H
