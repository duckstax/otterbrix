#ifndef otterbrix_cursor_H
#define otterbrix_cursor_H

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

typedef struct error_message {
    int32_t error_code;
    string_view_t message;
} error_message;

typedef void* doc_ptr;

class cursor_storage_t {
    public:
        cursor_storage_t(components::cursor::cursor_t_ptr cursor);

        int size();
        bool has_next();
        doc_ptr next();
        doc_ptr get();
        doc_ptr get(size_t index);
        bool is_success();
        bool is_error();
        error_message get_error();

    private:
        components::cursor::cursor_t_ptr cursor_;
};

#ifdef __cplusplus
}
#endif

#endif //otterbrix_cursor_H