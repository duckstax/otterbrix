#include "encoder.hpp"
#include "storage_impl.hpp"
#include "pointer.hpp"
#include "shared_keys.hpp"
#include "mutable_dict.h"
#include "endian.hpp"
#include "varint.hpp"
#include "exception.hpp"
#include "parse_date.hpp"
#include "platform_compat.hpp"
#include "temp_array.hpp"
#include <algorithm>
#include <cmath>
#include <float.h>
#include <stdlib.h>
#include "better_assert.hpp"

namespace document { namespace impl {

using namespace internal;

const slice_t encoder_t::pre_encoded_true       = {value_t::true_value,  size_narrow};
const slice_t encoder_t::pre_encoded_false      = {value_t::false_value, size_narrow};
const slice_t encoder_t::pre_encoded_null       = {value_t::null_value,  size_narrow};
const slice_t encoder_t::pre_encoded_empty_dict = {dict_t::empty_dict,   size_narrow};


encoder_t::encoder_t(size_t reserve_size)
    : _out(reserve_size)
    , _stack(initial_stack_size)
    , _strings(20)
{
    init();
}

encoder_t::encoder_t(FILE *output_file)
    : _out(output_file)
    , _stack(initial_stack_size)
    , _strings(10)
{
    init();
}

encoder_t::~encoder_t() = default;

void encoder_t::init() {
    reset_stack();
    _items->reset(tag_special);
    _items->reserve(1);
}

void encoder_t::reset_stack() {
    _items = &_stack[0];
    _stack_depth = 1;
}

void encoder_t::reset() {
    if (_items)
        _items->clear();
    _out.reset();
    _strings.clear();
    _string_storage.reset();
    _writing_key = _blocked_on_key = false;
    reset_stack();
    set_base(null_slice);
}

void encoder_t::set_shared_keys(shared_keys_t *s) {
    _shared_keys = s;
}

void encoder_t::suppress_trailer() {
    _trailer = false;
}

void encoder_t::write_raw(slice_t s) {
    _out.write(s);
}

void encoder_t::unique_strings(bool b) {
    _unique_strings = b;
}

void encoder_t::set_base(slice_t base, bool mark_extern_pointers, size_t cutoff) {
    _throw_if(_base && base, error_code::encode_error, "There's already a base");
    _base = base;
    _owned_base = null_slice;
    _base_cutoff = nullptr;
    if (base && cutoff > 0 && cutoff < base.size) {
        assert_precondition(cutoff >= 8);
        _base_cutoff = (char*)base.end() - cutoff;
    }
    _base_min_used = _base.end();
    _mark_extern_ptrs = mark_extern_pointers;
}

void encoder_t::end() {
    if (!_items)
        return;
    _throw_if(_stack_depth > 1, error_code::encode_error, "unclosed array/dict");
    _throw_if(_items->size() > 1, error_code::encode_error, "top level must have only one value");

    if (_trailer && !_items->empty()) {
        check_pointer_widths(_items, next_write_pos());
        fix_pointers(_items);
        value_t &root = (*_items)[0];
        if (_items->wide) {
            _out.write(&root, size_wide);
            new (_out.reserve_space(size_narrow)) pointer_t(4, size_narrow);
        } else {
            _out.write(&root, size_narrow);
        }
        _items->clear();
    }
    _out.flush();
    _items = nullptr;
    _stack_depth = 0;
}

size_t encoder_t::finish_item() {
    _throw_if(_stack_depth > 1, error_code::encode_error, "unclosed array/dict");
    _throw_if(!_items || _items->empty(), error_code::encode_error, "No item to end");

    size_t item_pos;
    const value_t *item = &(*_items)[0];
    if (item->is_pointer()) {
        item_pos = item->as_pointer()->offset<true>() - _base.size;
    } else {
        item_pos = next_write_pos();
        _out.write(item, (_items->wide ? size_wide : size_narrow));
    }
    _items->clear();
    reset_stack();
    return item_pos;
}

slice_t encoder_t::base() const {
    return _base;
}

slice_t encoder_t::base_used() const {
    return _base_min_used != 0 ? slice_t(_base_min_used, _base.end()) : slice_t();
}

const string_table_t &encoder_t::strings() const {
    return _strings;
}

alloc_slice_t encoder_t::finish() {
    end();
    alloc_slice_t out = _out.finish();
    if (out.size == 0)
        out.reset();
    return out;
}

retained_t<doc_t> encoder_t::finish_doc() {
    retained_t<doc_t> doc = new doc_t(finish(), doc_t::trust_type::trusted, _shared_keys, (_mark_extern_ptrs ? _base : slice_t()));
    return doc;
}

size_t encoder_t::next_write_pos() {
    _out.pad_to_even_length();
    return _out.length();
}

encoder_t::pre_written_value encoder_t::last_value_written() const {
    if (!_items->empty())
        if (auto ptr = _items->back().as_pointer(); ptr->is_pointer())
            return pre_written_value(ptr->offset<true>());
    return pre_written_value::none;
}

void encoder_t::write_value_again(pre_written_value pos) {
    _throw_if(pos == pre_written_value::none, error_code::encode_error, "Can't rewrite an inline value_t");
    write_pointer(ssize_t(pos) - _base.size);
}


alloc_slice_t encoder_t::snip() {
    _throw_if(_base, error_code::encode_error, "Can't snip when there's already a base");
    auto pos = last_value_written();
    if (pos == pre_written_value::none)
        return null_slice;

    auto offset = _out.length() - size_t(pos);
    new (_out.reserve_space(size_narrow)) pointer_t(offset, size_narrow);

    alloc_slice_t result = _out.finish();
    set_base(result, true);
    _owned_base = result;
    return result;
}

uint8_t* encoder_t::place_item() {
    _throw_if(_blocked_on_key, error_code::encode_error, "need a key before this value");
    if (_writing_key) {
        _writing_key = false;
    } else {
        if (_items->tag == tag_dict)
            _blocked_on_key = _writing_key = true;
    }
    return (uint8_t*) _items->push_back_new();
}

template <bool can_inline>
uint8_t* encoder_t::place_value(size_t size) {
    byte *buf;
    if (can_inline && size <= 4) {
        buf = place_item();
        if (size < 4)
            buf[2] = buf[3] = 0;
        if (size > 2)
            _items->wide = true;
        return buf;
    } else {
        write_pointer(next_write_pos());
        bool pad = (size & 1);
        buf = _out.reserve_space<byte>(size + pad);
        if (pad)
            buf[size] = 0;
    }
    return buf;
}

template <bool can_inline>
uint8_t* encoder_t::place_value(tags tag, byte param, size_t size) {
    assert_precondition(param <= 0x0F);
    byte *buf = place_value<can_inline>(size);
    buf[0] = byte((tag << 4) | param);
    return buf;
}

void encoder_t::add_special(int special_value) {
    new (place_item()) value_t(tag_special, special_value);
}

void encoder_t::write_null() {
    add_special(special_value_null);
}

void encoder_t::write_undefined() {
    add_special(special_value_undefined);
}

void encoder_t::write_bool(bool b) {
    add_special(b ? special_value_true : special_value_false);
}

void encoder_t::write_int(uint64_t i, bool is_short, bool is_unsigned) {
    if (is_short) {
        new (place_item()) value_t(tag_short, (i >> 8) & 0x0F, i & 0xFF);
    } else {
        byte intbuf[10];
        auto size = put_int_of_length(intbuf, i, is_unsigned);
        byte *buf = place_value<false>(tag_int, byte(size - 1), 1 + size);
        if (is_unsigned)
            buf[0] |= 0x08;
        memcpy(buf + 1, intbuf, size);
    }
}

void encoder_t::write_int(int64_t i) {
    write_int(i, (i < 2048 && i >= -2048), false);
}

void encoder_t::write_uint(uint64_t i) {
    write_int(i, (i < 2048), true);
}

void encoder_t::write_double(double d) {
    _throw_if(std::isnan(d), error_code::invalid_data, "Can't write NaN");
    if (is_float_representable(d)) {
        return _write_float((float)d);
    } else {
        endian::little_double swapped = d;
        auto buf = place_value<false>(tag_float, 0x08, 2 + sizeof(swapped));
        buf[1] = 0;
        memcpy(&buf[2], &swapped, sizeof(swapped));
    }
}

void encoder_t::write_string(slice_t s) {
    (void)_write_string(s);
}

void encoder_t::write_float(float f) {
    _throw_if(std::isnan(f), error_code::invalid_data, "Can't write NaN");
    _write_float(f);
}

void encoder_t::_write_float(float f) {
    endian::little_float swapped = f;
    auto buf = place_value<false>(tag_float, 0, 2 + sizeof(swapped));
    buf[1] = 0;
    memcpy(&buf[2], &swapped, sizeof(swapped));
}

bool encoder_t::is_float_representable(double n) noexcept {
    return (fabs(n) <= FLT_MAX && n == (float)n);
}

const void* encoder_t::write_data(tags tag, slice_t s) {
    byte *buf;
    if (s.size < size_narrow) {
        buf = place_value<true>(tag, byte(s.size), 1 + s.size);
        buf[1] = s.size > 0 ? s[0] : 0;
        buf = nullptr;
    } else {
        size_t bufLen = 1 + s.size;
        if (s.size >= 0x0F)
            bufLen += size_of_var_int(s.size);
        buf = place_value<false>(tag, 0, bufLen);
        if (s.size < 0x0F) {
            *buf++ |= byte(s.size);
        } else {
            *buf++ |= 0x0F;
            buf += put_uvar_int(buf, s.size);
        }
        memcpy(buf, s.buf, s.size);
        if (_out.output_file())
            buf = nullptr;
    }
    return buf;
}

const void* encoder_t::_write_string(slice_t s) {
    if (!_usually_true(_unique_strings && s.size >= size_narrow && s.size <= max_shared_string_size)) {
        return write_data(tag_string, s);
    }
    string_table_t::entry_t *entry;
    bool is_new;
    std::tie(entry, is_new) = _strings.insert(s, 0);
    if (!is_new) {
        ssize_t offset = entry->second - _base.size;
        if (_items->wide || next_write_pos() - offset <= pointer_t::max_narrow_offset - 32) {
            write_pointer(offset);
            if (offset < 0) {
                const void *string_value = &_base[_base.size + offset];
                if (string_value < _base_min_used)
                    _base_min_used = string_value;
            }
            return entry->first.buf;
        }
    }

    auto offset = _base.size + next_write_pos();
    _throw_if(offset > 1u<<31, error_code::memory_error, "encoded data too large");
    write_data(tag_string, s);

    const void* written_str = _string_storage.write(s);
    *entry = {{written_str, s.size}, (uint32_t)offset};
    return written_str;
}

void encoder_t::cache_string(slice_t s, size_t offset_in_base) {
    if (_usually_true(_unique_strings && s.size >= size_narrow && s.size <= max_shared_string_size))
        _strings.insert(s, uint32_t(offset_in_base));
}

void encoder_t::write_data(slice_t s) {
    write_data(tag_binary, s);
}

void encoder_t::write_value(const value_t *v) {
    write_value(v, nullptr);
}

void encoder_t::write_value(const value_t *v, encoder_t::func_write_value fn) {
    write_value(v, &fn);
}

void encoder_t::reuse_base_strings() {
    reuse_base_strings(value_t::from_trusted_data(_base));
}

void encoder_t::reuse_base_strings(const value_t *value) {
    if (value < _base_cutoff)
        return;
    switch (value->tag()) {
    case tag_string:
        cache_string(value->as_string(), size_t(value) - size_t(_base.buf));
        break;
    case tag_array:
        for (array_t::iterator iter(value->as_array()); iter; ++iter)
            reuse_base_strings(iter.value());
        break;
    case tag_dict:
        for (dict_t::iterator iter(value->as_dict()); iter; ++iter) {
            reuse_base_strings(iter.key());
            reuse_base_strings(iter.value());
        }
        break;
    default:
        break;
    }
}

void encoder_t::write_date_string(int64_t timestamp, bool utc) {
    char str[formatted_iso8601_date_max_size];
    write_string(format_iso8601_date(str, timestamp, utc));
}

bool encoder_t::is_narrow_value(const value_t *value) {
    if (value->tag() >= tag_array)
        return value->count_is_zero();
    else
        return value->data_size() <= size_narrow;
}

const value_t* encoder_t::min_used(const value_t *value) {
    if (value < _base_cutoff)
        return nullptr;
    switch (value->type()) {
    case value_type::array: {
        const value_t *min_value = value;
        for (array_t::iterator i((const array_t*)value); i; ++i) {
            min_value = std::min(min_value, min_used(i.value()));
            if (min_value == nullptr)
                break;
        }
        return min_value;
    }
    case value_type::dict: {
        const value_t *min_value = value;
        for (dict_t::iterator i((const dict_t*)value, false); i; ++i) {
            min_value = std::min(min_value, min_used(i.key()));
            min_value = std::min(min_value, min_used(i.value()));
            if (min_value == nullptr)
                break;
        }
        return min_value;
    }
    default:
        return value;
    }
}

void encoder_t::write_value(const value_t *value, const shared_keys_t* &sk, const func_write_value *fn) {
    if (value_in_base(value) && !is_narrow_value(value)) {
        auto min_value = min_used(value);
        if (min_value >= _base_cutoff) {
            write_pointer( (ssize_t)value - (ssize_t)_base.end() );
            if (min_value && min_value < _base_min_used)
                _base_min_used = min_value;
            return;
        }
    }
    switch (value->tag()) {
    case tag_short:
    case tag_int:
    case tag_float:
    case tag_special: {
        auto size = value->data_size();
        memcpy(place_value<true>(size), value, size);
        break;
    }
    case tag_string:
        write_string(value->as_string());
        break;
    case tag_binary:
        write_data(value->as_data());
        break;
    case tag_array: {
        ++_copying_collection;
        auto iter = value->as_array()->begin();
        begin_array(iter.count());
        for (; iter; ++iter) {
            if (!fn || !(*fn)(nullptr, iter.value()))
                write_value(iter.value(), sk, fn);
        }
        end_array();
        --_copying_collection;
        break;
    }
    case tag_dict: {
        ++_copying_collection;
        auto dict = (const dict_t*)value;
        if (dict->is_mutable()) {
            dict->heap_dict()->write_to(*this);
        } else {
            auto iter = dict->begin();
            begin_dict(iter.count());
            for (; iter; ++iter) {
                if (!fn || !(*fn)(iter.key(), iter.value())) {
                    if (!sk && iter.key()->is_int())
                        sk = value->shared_keys();
                    write_key(iter.key(), sk);
                    write_value(iter.value(), sk, fn);
                }
            }
            end_dict();
        }
        --_copying_collection;
        break;
    }
    default:
        exception_t::_throw(error_code::unknown_value, "illegal tag in value_t; corrupt data?");
    }
}

void encoder_t::write_value(const value_t *value NONNULL, const func_write_value *fn) {
    const shared_keys_t *sk = nullptr;
    write_value(value, sk, fn);
}

bool encoder_t::value_in_base(const value_t *value) const {
    return _base && value >= _base.buf && value < (const void*)_base.end();
}

bool encoder_t::empty() const {
    return _out.length() == 0 && _stack_depth == 1 && _items->empty();
}

size_t encoder_t::bytes_written_size() const {
    return _out.length();
}

void encoder_t::write_pointer(size_t p)   {
    new (place_item()) pointer_t(_base.size + p, size_wide);
}

void encoder_t::check_pointer_widths(value_array_t *items, size_t write_pos) {
    if (!items->wide) {
        for (value_t &v : *items) {
            if (v.is_pointer()) {
                ssize_t pos = v.as_pointer()->offset<true>() - _base.size;
                if (write_pos - pos > pointer_t::max_narrow_offset) {
                    items->wide = true;
                    break;
                }
            }
            write_pos += size_narrow;
        }
    }
}

void encoder_t::fix_pointers(value_array_t *items) {
    auto pointer_origin = next_write_pos();
    int width = items->wide ? size_wide : size_narrow;
    for (value_t &v : *items) {
        if (v.is_pointer()) {
            ssize_t pos = v.as_pointer()->offset<true>() - _base.size;
            assert(pos < (ssize_t)pointer_origin);
            bool is_external = (pos < 0);
            v = pointer_t(pointer_origin - pos, width, is_external && _mark_extern_ptrs);
        }
        pointer_origin += width;
    }
}

void encoder_t::adding_key() {
    if (_usually_false(!_blocked_on_key)) {
        if (_items->tag == tag_dict)
            exception_t::_throw(error_code::encode_error, "need a value after a key");
        else
            exception_t::_throw(error_code::encode_error, "not writing a dictionary");
    }
    _blocked_on_key = false;
}

void encoder_t::write_key(slice_t s) {
    int encoded;
    if (_shared_keys && _shared_keys->encode_and_add(s, encoded)) {
        write_key(encoded);
        return;
    }
    adding_key();
    const void* key = _write_string(s);
    if (!key && _copying_collection)
        key = s.buf;
    added_key({key, s.size});
}

void encoder_t::write_key(int n) {
    assert_precondition(_shared_keys || n == dict_t::magic_parent_key || is_disable_necessary_shared_keys_check);
    adding_key();
    write_int(n);
    added_key(null_slice);
}

void encoder_t::write_key(const value_t *key, const shared_keys_t *sk) {
    if (key->is_int()) {
        int int_key = (int)key->as_int();
        if (!sk) {
            sk = key->shared_keys();
            _throw_if(!sk, error_code::encode_error, "Numeric key given without shared_keys_t");
        }
        if (sk == _shared_keys) {
            if (sk->is_unknown_key(int_key)) {
                _throw_if(!sk->decode(int_key), error_code::invalid_data, "Unrecognized integer key");
            }
            write_key(int_key);
        } else {
            slice_t key_slice = sk->decode(int_key);
            _throw_if(!key_slice, error_code::invalid_data, "Unrecognized integer key");
            write_key(key_slice);
        }
    } else {
        slice_t str = key->as_string();
        _throw_if(!str, error_code::invalid_data, "Key must be a string or integer");
        int encoded;
        if (_shared_keys && _shared_keys->encode_and_add(str, encoded)) {
            write_key(encoded);
        } else {
            adding_key();
            write_value(key, nullptr);
            added_key(str);
        }
    }
}

void encoder_t::write_key(key_t key) {
    if (key.shared())
        write_key(key.as_int());
    else
        write_key(key.as_string());
}

void encoder_t::added_key(slice_t_c str) {
    _items->keys.push_back(str);
}

void encoder_t::push(tags tag, size_t reserve) {
    if (_usually_false(_stack_depth == 0))
        reset();
    if (_usually_false(_stack_depth >= _stack.size()))
        _stack.resize(2*_stack_depth);
    _items = &_stack[_stack_depth++];
    _items->reset(tag);
    if (reserve > 0) {
        if (_usually_true(tag == tag_dict)) {
            _items->reserve(2 * reserve);
            _items->keys.reserve(reserve);
        } else {
            _items->reserve(reserve);
        }
    }
}

void encoder_t::pop() {
    _throw_if(_stack_depth <= 1, error_code::internal_error, "encoder_t stack underflow!");
    --_stack_depth;
    _items = &_stack[_stack_depth - 1];
}

void encoder_t::begin_array(size_t reserve) {
    push(tag_array, reserve);
}

void encoder_t::begin_dict(size_t reserve) {
    push(tag_dict, 2*reserve);
    _writing_key = _blocked_on_key = true;
}

void encoder_t::begin_dict(const dict_t *parent, size_t reserve) {
    _throw_if(!value_in_base(parent), error_code::encode_error, "parent is not in base");
    begin_dict(1 + reserve);
    write_key(dict_t::magic_parent_key);
    write_value(parent);
}

void encoder_t::end_array() {
    end_collection(internal::tag_array);
}

void encoder_t::end_dict() {
    _throw_if(!_writing_key, error_code::encode_error, "need a value");
    end_collection(internal::tag_dict);
}

void encoder_t::end_collection(tags tag) {
    if (_usually_false(_items->tag != tag)) {
        if (_items->tag == tag_special)
            exception_t::_throw(error_code::encode_error, "end_collection: not in a collection");
        else
            exception_t::_throw(error_code::encode_error, "ending wrong type of collection");
    }

    value_array_t *items = _items;
    pop();
    _writing_key = _blocked_on_key = false;

    size_t n_values = items->size();
    auto count = uint32_t(n_values);
    if (_usually_true(count > 0)) {
        if (_usually_true(tag == tag_dict)) {
            count /= 2;
            sort_dict(*items);
        }

        size_t buf_len = 2;
        if (count >= long_array_count)
            buf_len += size_of_var_int(count - long_array_count);
        uint32_t inline_count = std::min(count, (uint32_t)long_array_count);
        byte *buf = place_value<false>(tag, byte(inline_count >> 8), buf_len);
        buf[1]  = (byte)(inline_count & 0xFF);
        if (count >= long_array_count)
            put_uvar_int(&buf[2], count - long_array_count);

        check_pointer_widths(items, next_write_pos());
        if (items->wide)
            buf[0] |= 0x08;

        fix_pointers(items);

        if (items->wide) {
            _out.write(&(*items)[0], size_wide*n_values);
        } else {
            auto narrow = _out.reserve_space<uint16_t>(n_values);
            for (auto &v : *items)
                ::memcpy(narrow++, &v, size_narrow);
        }
    } else {
        byte *buf = place_value<true>(tag, 0, 2);
        buf[1] = 0;
    }
    items->clear();
}

static inline int compare_keys_by_index(const slice_t_c *sa, const slice_t_c *sb) {
    if (sa->buf) {
        if (sb->buf)
            return compare_slice_c(*sa, *sb) < 0;
        else
            return false;
    } else {
        if (sb->buf)
            return true;
        else
            return (int)sa->size < (int)sb->size;
    }
}

void encoder_t::sort_dict(value_array_t &items) {
    auto &keys = items.keys;
    auto n = keys.size();
    if (n < 2)
        return;

    for (unsigned i = 0; i < n; i++) {
        if (keys[i].buf == nullptr) {
            const value_t *item = &items[2*i];
            if (item->tag() == tag_string) {
                keys[i].buf = offsetby(item, 1);
            } else {
                assert(item->tag() == tag_short);
                keys[i] = {nullptr, size_t(item->as_unsigned())};
            }
        }
    }

    _temp_array(indices, const slice_t_c*, n);
    const slice_t_c* base = &keys[0];
    for (unsigned i = 0; i < n; i++)
        indices[i] = base + i;
    std::sort(&indices[0], &indices[n], &compare_keys_by_index);
    _temp_array(oldBuf, char, 2*n * sizeof(value_t));
    auto old = (value_t*)oldBuf;
    memcpy(old, &items[0], 2*n * sizeof(value_t));
    for (size_t i = 0; i < n; i++) {
        auto j = indices[i] - base;
        if ((ssize_t)i != j) {
            items[2*i]   = old[2*j];
            items[2*i+1] = old[2*j+1];
        }
    }
}

} }
