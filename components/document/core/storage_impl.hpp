#pragma once

#include "value.hpp"
#include "array.hpp"
#include "dict.hpp"
#include "encoder.hpp"
#include "shared_keys.hpp"
#include "json_coder.hpp"
#include "exception.hpp"
#include <memory>

namespace storage { namespace impl {

enum class encode_format {
    internal,
    json
};


struct encoder_impl_t {
    storage::error_code error {storage::error_code::no_error};
    const bool owns_encoder {true};
    std::string error_message;
    std::unique_ptr<encoder_t> encoder;
    std::unique_ptr<json_encoder_t> json_encoder;
    void* extra_info {nullptr};

    encoder_impl_t(encode_format format, size_t reserve_size = 0, bool unique_strings = true);
    encoder_impl_t(FILE *output_file, bool unique_strings = true);
    encoder_impl_t(encoder_t *encoder);
    ~encoder_impl_t();

    bool is_internal() const;
    bool has_error() const;
    void record_exception(const std::exception &x) noexcept;
    void reset();
};


static void record_error(const std::exception &x, storage::error_code *out_error) noexcept {
    if (out_error)
        *out_error = exception_t::get_code(x);
}


inline encoder_impl_t::encoder_impl_t(encode_format format, size_t reserve_size, bool unique_strings) {
    if (reserve_size == 0)
        reserve_size = 256;
    if (format == encode_format::internal) {
        encoder.reset(new encoder_t(reserve_size));
        encoder->unique_strings(unique_strings);
    } else {
        json_encoder.reset(new json_encoder_t(reserve_size));
    }
}

inline encoder_impl_t::encoder_impl_t(FILE *output_file, bool unique_strings) {
    encoder.reset(new encoder_t(output_file));
    encoder->unique_strings(unique_strings);
}

inline encoder_impl_t::encoder_impl_t(encoder_t *encoder)
    : owns_encoder(false)
    , encoder(encoder)
{}

inline encoder_impl_t::~encoder_impl_t() {
    if (!owns_encoder)
        encoder.release();
}

inline bool encoder_impl_t::is_internal() const {
    return encoder != nullptr;
}

inline bool encoder_impl_t::has_error() const {
    return error != storage::error_code::no_error;
}

inline void encoder_impl_t::record_exception(const std::exception &x) noexcept {
    if (!has_error()) {
        storage::impl::record_error(x, &error);
        error_message = x.what();
    }
}

inline void encoder_impl_t::reset() {
    if (encoder)
        encoder->reset();
    if (json_encoder) {
        json_encoder->reset();
    }
    error = storage::error_code::no_error;
    extra_info = nullptr;
}

} }
