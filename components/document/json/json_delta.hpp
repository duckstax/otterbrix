#pragma once

#include <string>

#include <components/document/core/slice.hpp>
#include <components/document/core/encoder.hpp>

namespace document { namespace impl {

class json_encoder_t;


class json_delta_t {
public:
    static size_t min_string_diff_length;
    static float text_diff_timeout;

    static alloc_slice_t create(const value_t *old, const value_t *nuu);
    static bool create(const value_t *old, const value_t *nuu, json_encoder_t &enc);

    static alloc_slice_t apply(const value_t *old, slice_t json_delta);
    static void apply(const value_t *old, slice_t json_delta, encoder_t &enc);

private:
    struct path_item;

    json_delta_t(json_encoder_t &enc);
    bool _write(const value_t *old, const value_t *nuu, path_item *path);

    json_delta_t(encoder_t &decoder);
    void _apply(const value_t *old, const value_t* NONNULL delta);
    void _apply_array(const value_t* old, const array_t* NONNULL delta);
    void _patch_array(const array_t* NONNULL old, const dict_t* NONNULL delta);
    void _patch_dict(const dict_t* NONNULL old, const dict_t* NONNULL delta);

    void write_path(path_item *path);
    static bool is_delta_deletion(const value_t *delta);
    static std::string create_string_delta(slice_t old_str, slice_t nuu_str);
    static std::string apply_string_delta(slice_t old_str, slice_t diff);

    json_encoder_t* _encoder;
    encoder_t* _decoder;
};

} }
