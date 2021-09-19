#pragma once

#include "encoder.hpp"
#include "doc.hpp"
#include "exception.hpp"
#include "slice.hpp"

namespace boost { namespace json {
class value;
} }

namespace storage { namespace impl {

class json_converter_t {
public:
    enum {
        no_error = 0,
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

private:
    void write_value(const boost::json::value &value);

    encoder_t &_encoder;
    int _json_error {no_error};
    error_code _error {error_code::no_error};
    std::string _error_message;
    size_t _error_pos {0};
    slice_t _input;
};

} }
