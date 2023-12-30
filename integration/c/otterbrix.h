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
    int code;
    char* message;
} error_message;

otterbrix_ptr otterbrix_create(config_t cfg, string_view_t database, string_view_t collection);

void otterbrix_destroy(otterbrix_ptr);

cursor_ptr execute_sql(otterbrix_ptr, string_view_t query);

void ReleaseCursor(cursor_ptr ptr);

int32_t CursorSize(cursor_ptr ptr);

bool CursorHasNext(cursor_ptr ptr);

doc_ptr CursorNext(cursor_ptr ptr);

doc_ptr CursorGet(cursor_ptr ptr);

doc_ptr CursorGetByIndex(cursor_ptr ptr, int index);

bool CursorIsSuccess(cursor_ptr ptr);

bool CursorIsError(cursor_ptr ptr);

error_message CursorGetError(cursor_ptr ptr);

void ReleaseDocument(doc_ptr ptr);

char* DocumentID(doc_ptr ptr);

bool DocumentIsValid(doc_ptr ptr);

bool DocumentIsArray(doc_ptr ptr);

bool DocumentIsDict(doc_ptr ptr);

int32_t DocumentCount(doc_ptr ptr);

bool DocumentIsExistByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsExistByIndex(doc_ptr ptr, int32_t index);

bool DocumentIsNullByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsNullByIndex(doc_ptr ptr, int32_t index);

bool DocumentIsBoolByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsBoolByIndex(doc_ptr ptr, int32_t index);

bool DocumentIsUlongByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsUlongByIndex(doc_ptr ptr, int32_t index);

bool DocumentIsLongByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsLongByIndex(doc_ptr ptr, int32_t index);

bool DocumentIsDoubleByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsDoubleByIndex(doc_ptr ptr, int32_t index);

bool DocumentIsStringByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsStringByIndex(doc_ptr ptr, int32_t index);

bool DocumentIsArrayByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsArrayByIndex(doc_ptr ptr, int32_t index);

bool DocumentIsDictByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentIsDictByIndex(doc_ptr ptr, int32_t index);

bool DocumentGetBoolByKey(doc_ptr ptr, string_view_t key_raw);

bool DocumentGetBoolByIndex(doc_ptr ptr, int32_t index);

uint64_t DocumentGetUlongByKey(doc_ptr ptr, string_view_t key_raw);

uint64_t DocumentGetUlongByIndex(doc_ptr ptr, int32_t index);

int64_t DocumentGetLongByKey(doc_ptr ptr, string_view_t key_raw);

int64_t DocumentGetLongByIndex(doc_ptr ptr, int32_t index);

double DocumentGetDoubleByKey(doc_ptr ptr, string_view_t key_raw);

double DocumentGetDoubleByIndex(doc_ptr ptr, int32_t index);

char* DocumentGetStringByKey(doc_ptr ptr, string_view_t key_raw);

char* DocumentGetStringByIndex(doc_ptr ptr, int32_t index);

doc_ptr DocumentGetArrayByKey(doc_ptr ptr, string_view_t key_raw);

doc_ptr DocumentGetArrayByIndex(doc_ptr ptr, int32_t index);

doc_ptr DocumentGetDictByKey(doc_ptr ptr, string_view_t key_raw);

doc_ptr DocumentGetDictByIndex(doc_ptr ptr, int32_t index);

#ifdef __cplusplus
}
#endif

#endif //otterbrix_otterbrix_H
