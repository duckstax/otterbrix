#include "json_delta.hpp"
#include <components/document/core/dict.hpp>
#include <components/document/json/json_coder.hpp>
#include <components/document/support/exception.hpp>
#include <components/document/support/temp_array.hpp>
#include <components/document/support/better_assert.hpp>
#include <components/document/support/diff_match_patch.hpp>
#include <sstream>
#include <unordered_set>

namespace document { namespace impl {

bool compatible_deltas = false;
size_t json_delta_t::min_string_diff_length = 60;
float json_delta_t::text_diff_timeout = 0.25;

enum {
    code_deletion   = 0,
    code_text_diff  = 2,
    code_array_move = 3,
};

static inline bool is_utf8_continuation(uint8_t c) {
    return (c & 0xc0) == 0x80;
}

static void snap_to_utf8_character(long &pos, size_t &length, slice_t str);


struct json_delta_t::path_item {
    path_item *parent;
    bool is_open;
    slice_t key;
};


alloc_slice_t json_delta_t::create(const value_t *old, const value_t *nuu) {
    json_encoder_t enc;
    create(old, nuu, enc);
    return enc.finish();
}

bool json_delta_t::create(const value_t *old, const value_t *nuu, json_encoder_t &enc) {
    if (json_delta_t(enc)._write(old, nuu, nullptr))
        return true;
    enc.begin_dict();
    enc.end_dict();
    return false;
}

json_delta_t::json_delta_t(json_encoder_t &enc)
    : _encoder(&enc)
{}

void json_delta_t::write_path(path_item *path) {
    if (!path)
        return;
    write_path(path->parent);
    path->parent = nullptr;
    if (!path->is_open) {
        _encoder->begin_dict();
        path->is_open = true;
    }
    _encoder->write_key(path->key);
}

bool json_delta_t::_write(const value_t *old, const value_t *nuu, path_item *path) {
    if (_usually_false(old == nuu))
        return false;
    if (old) {
        if (!nuu) {
            write_path(path);
            _encoder->begin_array();
            if (compatible_deltas) {
                _encoder->write_value(old);
                _encoder->write_int(0);
                _encoder->write_int(code_deletion);
            }
            _encoder->end_array();
            return true;
        }

        auto old_type = old->type(), nuu_type = nuu->type();
        if (old_type == nuu_type) {
            if (old_type == value_type::dict) {
                auto old_dict = (const dict_t*)old, nuu_dict = (const dict_t*)nuu;
                path_item cur_level = {path, false, null_slice};
                unsigned old_keys_seen = 0;
                for (dict_t::iterator i_nuu(nuu_dict); i_nuu; ++i_nuu) {
                    slice_t key = i_nuu.key_string();
                    auto old_value = old_dict->get(key);
                    if (old_value)
                        ++old_keys_seen;
                    cur_level.key = key;
                    _write(old_value, i_nuu.value(), &cur_level);
                }
                if (old_keys_seen < old_dict->count()) {
                    for (dict_t::iterator it_old(old_dict); it_old; ++it_old) {
                        slice_t key = it_old.key_string();
                        if (nuu_dict->get(key) == nullptr) {
                            cur_level.key = key;
                            _write(it_old.value(), nullptr, &cur_level);
                        }
                    }
                }
                if (!cur_level.is_open)
                    return false;
                _encoder->end_dict();
                return true;

            } else if (old_type == value_type::array) {
                auto old_array = (const array_t*)old, nuu_array = (const array_t*)nuu;
                auto old_count = old_array->count(), nuu_count = nuu_array->count();
                auto min_count = std::min(old_count, nuu_count);
                if (min_count > 0) {
                    path_item cur_level = {path, false, null_slice};
                    uint32_t index = 0;
                    char key[10];
                    for (array_t::iterator it_old(old_array), it_new(nuu_array); index < min_count;
                         ++it_old, ++it_new, ++index) {
                        sprintf(key, "%d", index);
                        cur_level.key = slice_t(key);
                        _write(it_old.value(), it_new.value(), &cur_level);
                    }
                    if (old_count != nuu_count) {
                        sprintf(key, "%d-", index);
                        cur_level.key = slice_t(key);
                        write_path(&cur_level);
                        _encoder->begin_array();
                        for (; index < nuu_count; ++index) {
                            _encoder->write_value(nuu_array->get(index));
                        }
                        _encoder->end_array();
                    }
                    if (!cur_level.is_open)
                        return false;
                    _encoder->end_dict();
                    return true;
                } else if (old_count == 0 && nuu_count == 0) {
                    return false;
                }
            } else if (old->is_equal(nuu)) {
                return false;
            } else if (old_type == value_type::string && nuu_type == value_type::string) {
                std::string str_patch = create_string_delta(old->as_string(), nuu->as_string());
                if (!str_patch.empty()) {
                    write_path(path);
                    _encoder->begin_array();
                    _encoder->write_string(str_patch);
                    _encoder->write_int(0);
                    _encoder->write_int(code_text_diff);
                    _encoder->end_array();
                    return true;
                }
            }
        }
    }
    write_path(path);
    if (nuu->type() < value_type::array && path && !compatible_deltas) {
        _encoder->write_value(nuu);
    } else {
        _encoder->begin_array();
        if (compatible_deltas && old)
            _encoder->write_value(old);
        _encoder->write_value(nuu);
        _encoder->end_array();
    }
    return true;
}

alloc_slice_t json_delta_t::apply(const value_t *old, slice_t json_delta) {
    encoder_t enc;
    apply(old, json_delta, enc);
    return enc.finish();
}

void json_delta_t::apply(const value_t *old, slice_t json_delta, encoder_t &enc) {
    assert_precondition(json_delta);
    auto sk = old->shared_keys();
    alloc_slice_t data = json_coder::from_json(json_delta, sk);
    scope_t scope(data, sk);
    const value_t *delta = value_t::from_trusted_data(data);
    json_delta_t(enc)._apply(old, delta);
}

json_delta_t::json_delta_t(encoder_t &decoder)
    : _decoder(&decoder)
{}

void json_delta_t::_apply(const value_t *old, const value_t *delta) {
    switch(delta->type()) {
    case value_type::array:
        _apply_array(old, (const array_t*)delta);
        break;
    case value_type::dict: {
        auto delta_dict = (const dict_t*)delta;
        switch (old ? old->type() : value_type::null) {
        case value_type::array:
            _patch_array((const array_t*)old, delta_dict);
            break;
        case value_type::dict:
            _patch_dict((const dict_t*)old, delta_dict);
            break;
        default:
            if (delta_dict->empty() && old)
                _decoder->write_value(old);
            else
                exception_t::_throw(error_code::invalid_data, "Invalid {...} in delta");
        }
        break;
    }
    default:
        _decoder->write_value(delta);
        break;
    }
}

inline void json_delta_t::_apply_array(const value_t *old, const array_t* NONNULL delta) {
    switch (delta->count()) {
    case 0:
        _throw_if(!old, error_code::invalid_data, "Invalid deletion in delta");
        _decoder->write_value(value_t::undefined_value);
        break;
    case 1:
        _decoder->write_value(delta->get(0));
        break;
    case 2:
        _throw_if(!old, error_code::invalid_data, "Invalid replace in delta");
        _decoder->write_value(delta->get(1));
        break;
    case 3: {
        switch(delta->get(2)->as_int()) {
        case code_deletion:
            _throw_if(!old, error_code::invalid_data, "Invalid deletion in delta");
            _decoder->write_value(value_t::undefined_value);
            break;
        case code_text_diff: {
            slice_t old_str;
            if (old)
                old_str = old->as_string();
            _throw_if(!old_str, error_code::invalid_data, "Invalid text replace in delta");
            slice_t diff = delta->get(0)->as_string();
            _throw_if(diff.size == 0, error_code::invalid_data, "Invalid text diff in delta");
            auto nuu_str = apply_string_delta(old_str, diff);
            _decoder->write_string(nuu_str);
            break;
        }
        default:
            exception_t::_throw(error_code::invalid_data, "Unknown mode in delta");
        }
        break;
    }
    default:
        exception_t::_throw(error_code::invalid_data, "Bad array count in delta");
    }
}

inline void json_delta_t::_patch_dict(const dict_t* NONNULL old, const dict_t* NONNULL delta) {
    if (_decoder->value_in_base(old)) {
        _decoder->begin_dict(old);
        for (dict_t::iterator i(delta); i; ++i) {
            _decoder->write_key(i.key_string());
            _apply(old->get(i.key()->to_string()), i.value());
        }
        _decoder->end_dict();
    } else {
        _decoder->begin_dict();
        unsigned delta_keys_used = 0;
        for (dict_t::iterator i(old); i; ++i) {
            const value_t *value_delta = delta->get(i.key()->to_string());
            if (value_delta)
                ++delta_keys_used;
            if (!is_delta_deletion(value_delta)) {
                _decoder->write_key(i.key_string());
                auto old_value = i.value();
                if (value_delta == nullptr)
                    _decoder->write_value(old_value);
                else
                    _apply(old_value, value_delta);
            }
        }
        if (delta_keys_used < delta->count()) {
            for (dict_t::iterator i(delta); i; ++i) {
                if (old->get(i.key()->to_string()) == nullptr) {
                    _decoder->write_key(i.key_string());
                    _apply(nullptr, i.value());
                }
            }
        }
        _decoder->end_dict();
    }
}

inline void json_delta_t::_patch_array(const array_t* NONNULL old, const dict_t* NONNULL delta) {
    _decoder->begin_array();
    uint32_t index = 0;
    const value_t *remainder = nullptr;
    for (array_t::iterator it_old(old); it_old; ++it_old, ++index) {
        auto old_item = it_old.value();
        char key[10];
        sprintf(key, "%d", index);
        auto replacement = delta->get(slice_t(key));
        if (replacement) {
            _apply(old_item, replacement);
        } else {
            strcat(key, "-");
            remainder = delta->get(slice_t(key));
            if (remainder) {
                break;
            } else {
                _decoder->write_value(old_item);
            }
        }
    }
    if (!remainder) {
        char key[10];
        sprintf(key, "%d-", old->count());
        remainder = delta->get(slice_t(key));
    }
    if (remainder) {
        auto remainder_array = remainder->as_array();
        _throw_if(!remainder_array, error_code::invalid_data, "Invalid array remainder in delta");
        for (array_t::iterator it_rem(remainder_array); it_rem; ++it_rem)
            _decoder->write_value(it_rem.value());
    }
    _decoder->end_array();
}

inline bool json_delta_t::is_delta_deletion(const value_t *delta) {
    if (!delta)
        return false;
    auto array = delta->as_array();
    if (!array)
        return false;
    auto count = array->count();
    return count == 0 || (count == 3 && array->get(2)->as_int() == code_deletion);
}

std::string json_delta_t::create_string_delta(slice_t old_str, slice_t nuu_str) {
    if (nuu_str.size < min_string_diff_length
            || (compatible_deltas && old_str.size > min_string_diff_length))
        return "";
    diff_match_patch<std::string> dmp;
    dmp.diff_timeout = text_diff_timeout;
    auto patches = dmp.patch_make(std::string(old_str), std::string(nuu_str));

    if (compatible_deltas)
        return dmp.patch_to_text(patches);

    std::stringstream diff;
    long last_old_pos = 0, correction = 0;
    for (auto patch = patches.begin(); patch != patches.end(); ++patch) {
        long old_pos = patch->start1 + correction;
        long nuu_pos = patch->start2;
        auto &diffs = patch->diffs;
        for (auto cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            auto length = cur_diff->text.length();
            if (cur_diff->operation == diff_match_patch<std::string>::operation_t::equal) {
                old_pos += static_cast<long>(length);
                nuu_pos += static_cast<long>(length);
            } else {
                if (cur_diff->operation == diff_match_patch<std::string>::operation_t::remove)
                    snap_to_utf8_character(old_pos, length, old_str);
                else
                    snap_to_utf8_character(nuu_pos, length, nuu_str);

                assert(old_pos >= last_old_pos);
                if (old_pos > last_old_pos) {
                    diff << (old_pos-last_old_pos) << '=';
                }
                if (cur_diff->operation == diff_match_patch<std::string>::operation_t::remove) {
                    diff << length << '-';
                    old_pos += static_cast<long>(length);
                } else {
                    diff << length << '+';
                    diff.write((const char*)&nuu_str[nuu_pos], length);
                    diff << '|';
                    nuu_pos += static_cast<long>(length);
                }
                last_old_pos = old_pos;
            }
            if ((size_t)diff.tellp() + 6 >= nuu_str.size)
                return "";
        }
        correction += patch->length1 - patch->length2;
    }
    if (old_str.size > size_t(last_old_pos)) {
        diff << (old_str.size-last_old_pos) << '=';
    }
    return diff.str();
}

std::string json_delta_t::apply_string_delta(slice_t old_str, slice_t diff) {
    std::stringstream in{std::string(diff)};
    in.exceptions(std::stringstream::failbit | std::stringstream::badbit);
    std::stringstream nuu;
    unsigned pos = 0;
    while (in.peek() >= 0) {
        char op;
        unsigned len;
        in >> len;
        in >> op;
        switch (op) {
        case '=':
            _throw_if(pos + len > old_str.size, error_code::invalid_data, "Invalid length in text delta");
            nuu.write((const char*)&old_str[pos], len);
            pos += len;
            break;
        case '-':
            pos += len;
            break;
        case '+': {
            _temp_array(insertion, char, len);
            in.read(insertion, len);
            nuu.write(insertion, len);
            in >> op;
            _throw_if(op != '|', error_code::invalid_data, "Missing insertion delimiter in text delta");
            break;
        }
        default:
            exception_t::_throw(error_code::invalid_data, "Unknown op in text delta");
        }
    }
    _throw_if(pos != old_str.size, error_code::invalid_data, "Length mismatch in text delta");
    return nuu.str();
}

static void snap_to_utf8_character(long &pos, size_t &length, slice_t str) {
    while (is_utf8_continuation(str[pos])) {
        --pos;
        _throw_if(pos < 0, error_code::invalid_data, "Invalid UTF-8 at start of a string");
        ++length;
    }
    while (pos+length < str.size && is_utf8_continuation(str[pos+length])) {
        ++length;
    }
}

} }
