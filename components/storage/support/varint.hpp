#pragma once

#include "slice.hpp"

namespace storage {

enum {
    max_varint_len16 = 3,
    max_varint_len32 = 5,
    max_varint_len64 = 10,
};

size_t size_of_var_int(uint64_t n);
size_t put_uvar_int(void *buf NONNULL, uint64_t n);
size_t put_int_of_length(void *buf NONNULL, int64_t n, bool is_unsigned = false);
size_t _get_uvar_int(slice_t buf, uint64_t *n NONNULL);
size_t _get_uvar_int32(slice_t buf, uint32_t *n NONNULL);

static inline size_t get_uvar_int(slice_t buf, uint64_t *n NONNULL) {
    if (_usually_false(buf.size == 0))
        return 0;
    uint8_t byte = buf[0];
    if (_usually_true(byte < 0x80)) {
        *n = byte;
        return 1;
    }
    return _get_uvar_int(buf, n);
}

static inline size_t get_uvar_int32(slice_t buf, uint32_t *n NONNULL) {
    if (_usually_false(buf.size == 0))
        return 0;
    uint8_t byte = buf[0];
    if (_usually_true(byte < 0x80)) {
        *n = byte;
        return 1;
    }
    return _get_uvar_int32(buf, n);
}

}
