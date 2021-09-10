#pragma once

#include "encoder.hpp"
#include "doc.hpp"
#include "exception.hpp"
#include "slice.hpp"

extern "C" {
struct jsonsl_state_st;
struct jsonsl_st;
}

namespace storage { namespace impl {

class json_converter_t {
public:
    enum {
        error_truncated_json = 1000,
        error_exception_thrown
    };

    json_converter_t(encoder_t &enc) noexcept;
    ~json_converter_t();

    bool encode_json(slice_t json);
    int json_error() noexcept;
    error_code error() noexcept;
    const char* error_message() noexcept;
    size_t error_pos() noexcept;
    void reset();
    static alloc_slice_t convert_json(slice_t json, shared_keys_t *sk = nullptr);
    void push(struct jsonsl_state_st *state NONNULL);
    void pop(struct jsonsl_state_st *state NONNULL);
    int got_error(int err, size_t pos) noexcept;
    int got_error(int err, const char *errat) noexcept;
    void got_exception(error_code code, const char *what NONNULL, size_t pos) noexcept;

private:
    void write_double(struct jsonsl_state_st *state);

    encoder_t &_encoder;
    struct jsonsl_st * _jsn {nullptr};
    int _json_error {0};
    error_code _error {error_code::no_error};
    std::string _error_message;
    size_t _error_pos {0};
    slice_t _input;
};

} }
