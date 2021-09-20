#pragma once

#include <stdio.h>
#include "storage_impl.hpp"
#include "slice_core.hpp"
#include "value_slot.hpp"
#include "path.hpp"
#include "deep_iterator.hpp"
#include "doc.hpp"

namespace storage {

using encode_format = storage::impl::encode_format;
using trust_type = storage::impl::doc_t::trust_type;
using value_type = storage::impl::value_type;
using copy_flags = storage::impl::copy_flags;

using value_t_c = const storage::impl::value_t*;
using array_t_c = const storage::impl::array_t*;
using dict_t_c = const storage::impl::dict_t*;
using slot_t_c = storage::impl::value_slot_t*;
using mutable_array_t_c = storage::impl::mutable_array_t*;
using mutable_dict_t_c = storage::impl::mutable_dict_t*;
using encoder_t_c = storage::impl::encoder_impl_t*;
using shared_keys_t_c = storage::impl::shared_keys_t*;
using key_path_t_c = storage::impl::path_t*;
using deep_iterator_t_c = storage::impl::deep_iterator_t*;
using doc_t_c = const storage::impl::doc_t*;

using time_stamp = int64_t;
static constexpr time_stamp time_stamp_none = INT64_MIN;
extern const value_t_c null_value_c;
extern const array_t_c empty_array_c;
extern const dict_t_c empty_dict_c;

typedef struct _shared_key_scope* shared_key_scope;

struct array_iterator_t_c {
    void* _private1;
    uint32_t _private2;
    bool _private3;
    void* _private4;
};

struct dict_iterator_t_c {
    void* _private1;
    uint32_t _private2;
    bool _private3;
    void* _private4[4];
    int _private5;
};

struct dict_key_t_c {
    slice_t_c _private1;
    void* _private2;
    uint32_t _private3, private4;
    bool private5;
};

struct path_component_t_c {
    slice_t_c key;
    uint32_t index;
};


doc_t_c doc_from_result_data(slice_result_t_c data, trust_type trust, shared_keys_t_c sk, slice_t_c extern_data) noexcept;
doc_t_c doc_from_json(slice_t_c json, error_code *out_error) noexcept;
void doc_release(doc_t_c doc) noexcept;
doc_t_c doc_retain(doc_t_c doc) noexcept;

slice_t_c doc_get_data(doc_t_c doc) noexcept PURE;
slice_result_t_c doc_get_alloced_data(doc_t_c doc) noexcept PURE;
value_t_c doc_get_root(doc_t_c doc) noexcept PURE;
shared_keys_t_c doc_get_shared_keys(doc_t_c doc) noexcept PURE;

bool doc_set_associated(doc_t_c doc, void *pointer, const char *type) noexcept;
void* doc_get_associated(doc_t_c doc, const char *type) noexcept PURE;

doc_t_c value_find_doc(value_t_c v) noexcept PURE;
value_t_c value_from_data(slice_t_c data, trust_type trust) noexcept PURE;

slice_result_t_c data_convert_json(slice_t_c json, error_code *out_error) noexcept;
slice_result_t_c data_dump(slice_t_c data) noexcept;

slice_result_t_c value_to_json(value_t_c v, bool canonical_form = false) noexcept;

const char* dump(value_t_c v) noexcept;
const char* dump_data(slice_t_c data) noexcept;

value_type value_get_type(value_t_c v) noexcept PURE;

bool value_is_integer(value_t_c v) noexcept PURE;
bool value_is_unsigned(value_t_c v) noexcept PURE;
bool value_is_double(value_t_c v) noexcept;
bool value_as_bool(value_t_c v) noexcept PURE;

int64_t value_as_int(value_t_c v) noexcept PURE;
uint64_t value_as_unsigned(value_t_c v) noexcept PURE;
float value_as_float(value_t_c v) noexcept PURE;
double value_as_double(value_t_c v) noexcept PURE;
slice_t_c value_as_string(value_t_c v) noexcept PURE;

time_stamp value_as_timestamp(value_t_c) noexcept PURE;
slice_t_c value_as_data(value_t_c) noexcept PURE;
array_t_c value_as_array(value_t_c) noexcept PURE;
dict_t_c value_as_dict(value_t_c) noexcept PURE;
slice_result_t_c value_to_string(value_t_c) noexcept;

bool value_is_equals(value_t_c v1, value_t_c v2) noexcept PURE;
bool value_is_mutable(value_t_c v) noexcept PURE;

value_t_c value_retain(value_t_c v) noexcept;
void value_release(value_t_c v) noexcept;

static inline array_t_c array_retain(array_t_c v)   { value_retain((value_t_c)v); return v; }
static inline void array_release(array_t_c v)       { value_release((value_t_c)v); }
static inline dict_t_c dict_retain(dict_t_c v)      { value_retain((value_t_c)v); return v; }
static inline void dict_release(dict_t_c v)         { value_release((value_t_c)v); }

value_t_c value_new_string(slice_t_c str) noexcept;
value_t_c value_new_data(slice_t_c str) noexcept;

uint32_t array_count(array_t_c a) noexcept PURE;
bool array_is_empty(array_t_c a) noexcept PURE;
mutable_array_t_c array_as_mutable(array_t_c a) noexcept PURE;
value_t_c array_get(array_t_c a, uint32_t index) noexcept PURE;

void array_iterator_begin(array_t_c a, array_iterator_t_c *i NONNULL) noexcept;
value_t_c array_iterator_get_value(const array_iterator_t_c *i NONNULL) noexcept PURE;
value_t_c array_iterator_get_value_at(const array_iterator_t_c *i NONNULL, uint32_t offset) noexcept PURE;
uint32_t array_iterator_get_count(const array_iterator_t_c *i NONNULL) noexcept PURE;
bool array_iterator_next(array_iterator_t_c *i NONNULL) noexcept;

mutable_array_t_c array_mutable_copy(array_t_c a, copy_flags flags) noexcept;
mutable_array_t_c mutable_array_new(void) noexcept;

static inline mutable_array_t_c mutable_array_retain(mutable_array_t_c d) {
    return (mutable_array_t_c)value_retain((value_t_c)d);
}
static inline void mutable_array_release(mutable_array_t_c d) {
    value_release((value_t_c)d);
}

array_t_c mutable_array_get_source(mutable_array_t_c a) noexcept;
bool mutable_array_is_changed(mutable_array_t_c a) noexcept;
void mutable_array_set_changed(mutable_array_t_c a, bool c) noexcept;
void mutable_array_insert(mutable_array_t_c array, uint32_t first_index, uint32_t count) noexcept;
void mutable_array_remove(mutable_array_t_c array, uint32_t first_index, uint32_t count) noexcept;
void mutable_array_resize(mutable_array_t_c array, uint32_t size) noexcept;
mutable_array_t_c mutable_array_get_mutable_array(mutable_array_t_c a, uint32_t index) noexcept;
mutable_dict_t_c mutable_array_get_mutable_dict(mutable_array_t_c a, uint32_t index) noexcept;

static inline void mutable_array_set_null(mutable_array_t_c array NONNULL, uint32_t index);
static inline void mutable_array_set_bool(mutable_array_t_c array NONNULL, uint32_t index, bool value);
static inline void mutable_array_set_int(mutable_array_t_c array NONNULL, uint32_t index, int64_t value);
static inline void mutable_array_set_uint(mutable_array_t_c array NONNULL, uint32_t index, uint64_t value);
static inline void mutable_array_set_float(mutable_array_t_c array NONNULL, uint32_t index, float value);
static inline void mutable_array_set_double(mutable_array_t_c array NONNULL, uint32_t index, double value);
static inline void mutable_array_set_string(mutable_array_t_c array NONNULL, uint32_t index, slice_t_c value);
static inline void mutable_array_set_data(mutable_array_t_c array NONNULL, uint32_t index, slice_t_c value);
static inline void mutable_array_set_value(mutable_array_t_c array NONNULL, uint32_t index, value_t_c value);
static inline void mutable_array_set_array(mutable_array_t_c array NONNULL, uint32_t index, array_t_c value);
static inline void mutable_array_set_dict(mutable_array_t_c array NONNULL, uint32_t index, dict_t_c value);

static inline void mutable_array_append_null(mutable_array_t_c array NONNULL);
static inline void mutable_array_append_bool(mutable_array_t_c array NONNULL, bool value);
static inline void mutable_array_append_int(mutable_array_t_c array NONNULL, int64_t value);
static inline void mutable_array_append_uint(mutable_array_t_c array NONNULL, uint64_t value);
static inline void mutable_array_append_float(mutable_array_t_c array NONNULL, float value);
static inline void mutable_array_append_double(mutable_array_t_c array NONNULL, double value);
static inline void mutable_array_append_string(mutable_array_t_c array NONNULL, slice_t_c value);
static inline void mutable_array_append_data(mutable_array_t_c array NONNULL, slice_t_c value);
static inline void mutable_array_append_value(mutable_array_t_c array NONNULL, value_t_c value);
static inline void mutable_array_append_array(mutable_array_t_c array NONNULL, array_t_c value);
static inline void mutable_array_append_dict(mutable_array_t_c array NONNULL, dict_t_c value);

uint32_t dict_count(dict_t_c d) noexcept PURE;
bool dict_is_empty(dict_t_c d) noexcept PURE;
mutable_dict_t_c dict_as_mutable(dict_t_c d) noexcept PURE;
value_t_c dict_get(dict_t_c d, slice_t_c key_string) noexcept PURE;

void dict_iterator_begin(dict_t_c d, dict_iterator_t_c *i NONNULL) noexcept;
value_t_c dict_iterator_get_key(const dict_iterator_t_c *i NONNULL) noexcept PURE;
slice_t_c dict_iterator_get_key_string(const dict_iterator_t_c *i NONNULL) noexcept;
value_t_c dict_iterator_get_value(const dict_iterator_t_c *i NONNULL) noexcept PURE;
uint32_t dict_iterator_get_count(const dict_iterator_t_c *i  NONNULL) noexcept PURE;
bool dict_iterator_next(dict_iterator_t_c *i NONNULL) noexcept;
void dict_iterator_end(dict_iterator_t_c *i NONNULL) noexcept;

dict_key_t_c dict_key_init(slice_t_c string) noexcept;
slice_t_c dict_key_get_string(const dict_key_t_c *key NONNULL) noexcept;
value_t_c dict_get_with_key(dict_t_c d, dict_key_t_c *key NONNULL) noexcept;
mutable_dict_t_c dict_mutable_copy(dict_t_c source, copy_flags flags) noexcept;
mutable_dict_t_c mutable_dict_new(void) noexcept;

static inline mutable_dict_t_c mutable_dict_retain(mutable_dict_t_c d) {
    return (mutable_dict_t_c)value_retain((value_t_c)d);
}
static inline void mutable_dict_release(mutable_dict_t_c d) {
    value_release((value_t_c)d);
}

dict_t_c mutable_dict_get_source(mutable_dict_t_c d) noexcept;
bool mutable_dict_is_changed(mutable_dict_t_c d) noexcept;
void mutable_dict_set_changed(mutable_dict_t_c d, bool changed) noexcept;
void mutable_dict_remove(mutable_dict_t_c d, slice_t_c key) noexcept;
void mutable_dict_remove_all(mutable_dict_t_c d) noexcept;
mutable_array_t_c mutable_dict_get_mutable_array(mutable_dict_t_c d, slice_t_c key) noexcept;
mutable_dict_t_c mutable_dict_get_mutable_dict(mutable_dict_t_c d, slice_t_c key) noexcept;

static inline void mutable_dict_set_null(mutable_dict_t_c d NONNULL, slice_t_c key);
static inline void mutable_dict_set_bool(mutable_dict_t_c d NONNULL, slice_t_c key, bool value);
static inline void mutable_dict_set_int(mutable_dict_t_c d NONNULL, slice_t_c key, int64_t value);
static inline void mutable_dict_set_uint(mutable_dict_t_c d NONNULL, slice_t_c key, uint64_t value);
static inline void mutable_dict_set_float(mutable_dict_t_c d NONNULL, slice_t_c key, float value);
static inline void mutable_dict_set_double(mutable_dict_t_c d NONNULL, slice_t_c key, double value);
static inline void mutable_dict_set_string(mutable_dict_t_c d NONNULL, slice_t_c key, slice_t_c value);
static inline void mutable_dict_set_data(mutable_dict_t_c d NONNULL, slice_t_c key, slice_t_c value);
static inline void mutable_dict_set_value(mutable_dict_t_c d NONNULL, slice_t_c key, value_t_c value);
static inline void mutable_dict_set_array(mutable_dict_t_c d NONNULL, slice_t_c key, array_t_c value);
static inline void mutable_dict_set_dict(mutable_dict_t_c d NONNULL, slice_t_c key, dict_t_c value);

deep_iterator_t_c deep_iterator_new(value_t_c v) noexcept;
void deep_iterator_free(deep_iterator_t_c i) noexcept;
value_t_c deep_iterator_get_value(deep_iterator_t_c i NONNULL) noexcept;
value_t_c deep_iterator_get_parent(deep_iterator_t_c i NONNULL) noexcept;
slice_t_c deep_iterator_get_key(deep_iterator_t_c i NONNULL) noexcept;
uint32_t deep_iterator_get_index(deep_iterator_t_c i NONNULL) noexcept;
size_t deep_iterator_get_depth(deep_iterator_t_c i NONNULL) noexcept;
void deep_iterator_skip_children(deep_iterator_t_c i NONNULL) noexcept;
bool deep_iterator_next(deep_iterator_t_c i NONNULL) noexcept;
void deep_iterator_get_path(deep_iterator_t_c i NONNULL, path_component_t_c* * NONNULL out_path, size_t* NONNULL out_depth) noexcept;
slice_result_t_c deep_iterator_get_path_string(deep_iterator_t_c i NONNULL) noexcept;
slice_result_t_c deep_iterator_get_json_pointer(deep_iterator_t_c i NONNULL) noexcept;

key_path_t_c key_path_new(slice_t_c specifier, error_code *error) noexcept;
void key_path_free(key_path_t_c path) noexcept;
value_t_c key_path_eval(key_path_t_c path NONNULL, value_t_c root) noexcept;
value_t_c key_path_eval_once(slice_t_c specifier, value_t_c root NONNULL, error_code *error) noexcept;
slice_result_t_c key_path_to_string(key_path_t_c path) noexcept;
bool key_path_equals(key_path_t_c path1, key_path_t_c path2) noexcept;
bool key_path_get_element(key_path_t_c path NONNULL, size_t i, slice_t_c *out_key NONNULL, int32_t *out_index NONNULL) noexcept;

shared_keys_t_c shared_keys_new(void) noexcept;
slice_result_t_c shared_keys_get_state_data(shared_keys_t_c sk NONNULL) noexcept;
bool shared_keys_load_state_data(shared_keys_t_c sk, slice_t_c data) noexcept;
void shared_keys_write_state(shared_keys_t_c sk, encoder_t_c encoder) noexcept;
bool shared_keys_load_state(shared_keys_t_c sk, value_t_c v) noexcept;
int shared_keys_encode(shared_keys_t_c sk NONNULL, slice_t_c d, bool add) noexcept;
slice_t_c shared_keys_decode(shared_keys_t_c sk NONNULL, int key) noexcept;
unsigned shared_keys_count(shared_keys_t_c sk NONNULL) noexcept;
void shared_keys_revert_to_count(shared_keys_t_c sk NONNULL, unsigned old_count) noexcept;
shared_keys_t_c shared_keys_retain(shared_keys_t_c sk) noexcept;
void shared_keys_release(shared_keys_t_c sk) noexcept;

shared_key_scope shared_key_scope_with_range(slice_t_c range, shared_keys_t_c sk) noexcept;
void shared_key_scope_free(shared_key_scope scope);

encoder_t_c encoder_new(void) noexcept;
encoder_t_c encoder_new_with_options(encode_format format, size_t reserve_size, bool unique_strings) noexcept;
encoder_t_c encoder_new_writing_to_file(FILE *output_file NONNULL, bool unique_strings) noexcept;
void encoder_free(encoder_t_c encoder) noexcept;
void encoder_set_shared_keys(encoder_t_c encoder NONNULL, shared_keys_t_c sk) noexcept;
void encoder_set_extra_info(encoder_t_c encoder NONNULL, void *info) noexcept;
void* encoder_get_extra_info(encoder_t_c encoder NONNULL) noexcept;
void encoder_amend(encoder_t_c encoder NONNULL, slice_t_c base, bool reuse_strings, bool extern_pointers) noexcept;
slice_t_c encoder_get_base(encoder_t_c encoder NONNULL) noexcept;
void encoder_suppress_trailer(encoder_t_c encoder NONNULL) noexcept;
void encoder_reset(encoder_t_c encoder NONNULL) noexcept;
size_t encoder_bytes_written(encoder_t_c encoder NONNULL) noexcept;
size_t encoder_get_next_write_pos(encoder_t_c encoder NONNULL) noexcept;
bool encoder_write_null(encoder_t_c encoder NONNULL) noexcept;
bool encoder_write_undefined(encoder_t_c encoder NONNULL) noexcept;
bool encoder_write_bool(encoder_t_c encoder NONNULL, bool value) noexcept;
bool encoder_write_int(encoder_t_c encoder NONNULL, int64_t value) noexcept;
bool encoder_write_uint(encoder_t_c encoder NONNULL, uint64_t value) noexcept;
bool encoder_write_float(encoder_t_c encoder NONNULL, float value) noexcept;
bool encoder_write_double(encoder_t_c encoder NONNULL, double value) noexcept;
bool encoder_write_string(encoder_t_c encoder NONNULL, slice_t_c value) noexcept;
bool encoder_write_date_string(encoder_t_c encoder NONNULL, time_stamp ts, bool utc) noexcept;
bool encoder_write_data(encoder_t_c encoder NONNULL, slice_t_c value) noexcept;
bool encoder_write_raw(encoder_t_c encoder NONNULL, slice_t_c value) noexcept;
bool encoder_begin_array(encoder_t_c encoder NONNULL, size_t reserve_count) noexcept;
bool encoder_end_array(encoder_t_c encoder NONNULL) noexcept;
bool encoder_begin_dict(encoder_t_c encoder NONNULL, size_t reserve_count) noexcept;
bool encoder_write_key(encoder_t_c encoder NONNULL, slice_t_c key) noexcept;
bool encoder_write_key_value(encoder_t_c encoder NONNULL, value_t_c key NONNULL) noexcept;
bool encoder_end_dict(encoder_t_c encoder NONNULL) noexcept;
bool encoder_write_value(encoder_t_c encoder NONNULL, value_t_c value NONNULL) noexcept;
intptr_t encoder_last_value_written(encoder_t_c encoder);
void encoder_write_value_again(encoder_t_c encoder, intptr_t pre_written_value);
slice_result_t_c encoder_snip(encoder_t_c encoder);
bool encoder_convert_json(encoder_t_c encoder NONNULL, slice_t_c json) noexcept;
size_t encoder_finish_item(encoder_t_c encoder NONNULL) noexcept;
doc_t_c encoder_finish_doc(encoder_t_c encoder NONNULL, error_code *out_error) noexcept;
MUST_USE_RESULT
slice_result_t_c encoder_finish(encoder_t_c encoder, error_code *out_error) noexcept;
error_code encoder_get_error(encoder_t_c encoder NONNULL) noexcept;
const char* encoder_get_error_message(encoder_t_c encoder NONNULL) noexcept;

slice_result_t_c create_json_delta(value_t_c old, value_t_c nuu) noexcept;
bool encode_json_delta(value_t_c old, value_t_c nuu, encoder_t_c NONNULL jsonEncoder) noexcept;
slice_result_t_c apply_json_delta(value_t_c old, slice_t_c jsonDelta, error_code *error) noexcept;
bool encode_applying_json_delta(value_t_c old, slice_t_c jsonDelta, encoder_t_c encoder) noexcept;

MUST_USE_RESULT
slot_t_c mutable_array_set(mutable_array_t_c array NONNULL, uint32_t index) noexcept;
MUST_USE_RESULT
slot_t_c mutable_array_append(mutable_array_t_c array NONNULL) noexcept;
MUST_USE_RESULT
slot_t_c mutable_dict_set(mutable_dict_t_c dict NONNULL, slice_t_c key) noexcept;

void slot_set_null(slot_t_c slot NONNULL) noexcept;
void slot_set_bool(slot_t_c slot NONNULL, bool value) noexcept;
void slot_set_int(slot_t_c slot NONNULL, int64_t value) noexcept;
void slot_set_uint(slot_t_c slot NONNULL, uint64_t value) noexcept;
void slot_set_float(slot_t_c slot NONNULL, float value) noexcept;
void slot_set_double(slot_t_c slot NONNULL, double value) noexcept;
void slot_set_string(slot_t_c slot NONNULL, slice_t_c value) noexcept;
void slot_set_data(slot_t_c slot NONNULL, slice_t_c value) noexcept;
void slot_set_value(slot_t_c slot NONNULL, value_t_c value) noexcept;

static inline void slot_set_array(slot_t_c NONNULL slot, array_t_c array) {
    slot_set_value(slot, (value_t_c)array);
}
static inline void slot_set_dict(slot_t_c NONNULL slot, dict_t_c dict) {
    slot_set_value(slot, (value_t_c)dict);
}
static inline void mutable_array_set_null(mutable_array_t_c a, uint32_t index) {
    slot_set_null(mutable_array_set(a, index));
}
static inline void mutable_array_set_bool(mutable_array_t_c a, uint32_t index, bool val) {
    slot_set_bool(mutable_array_set(a, index), val);
}
static inline void mutable_array_set_int(mutable_array_t_c a, uint32_t index, int64_t val) {
    slot_set_int(mutable_array_set(a, index), val);
}
static inline void mutable_array_set_uint(mutable_array_t_c a, uint32_t index, uint64_t val) {
    slot_set_uint(mutable_array_set(a, index), val);
}
static inline void mutable_array_set_float(mutable_array_t_c a, uint32_t index, float val) {
    slot_set_float(mutable_array_set(a, index), val);
}
static inline void mutable_array_set_double(mutable_array_t_c a, uint32_t index, double val) {
    slot_set_double(mutable_array_set(a, index), val);
}
static inline void mutable_array_set_string(mutable_array_t_c a, uint32_t index, slice_t_c val) {
    slot_set_string(mutable_array_set(a, index), val);
}
static inline void mutable_array_set_data(mutable_array_t_c a, uint32_t index, slice_t_c val) {
    slot_set_data(mutable_array_set(a, index), val);
}
static inline void mutable_array_set_value(mutable_array_t_c a, uint32_t index, value_t_c val) {
    slot_set_value(mutable_array_set(a, index), val);
}
static inline void mutable_array_set_array(mutable_array_t_c a, uint32_t index, array_t_c val) {
    slot_set_value(mutable_array_set(a, index), (value_t_c)val);
}
static inline void mutable_array_set_dict(mutable_array_t_c a, uint32_t index, dict_t_c val) {
    slot_set_value(mutable_array_set(a, index), (value_t_c)val);
}
static inline void mutable_array_append_null(mutable_array_t_c a) {
    slot_set_null(mutable_array_append(a));
}
static inline void mutable_array_append_bool(mutable_array_t_c a, bool val) {
    slot_set_bool(mutable_array_append(a), val);
}
static inline void mutable_array_append_int(mutable_array_t_c a, int64_t val) {
    slot_set_int(mutable_array_append(a), val);
}
static inline void mutable_array_append_uint(mutable_array_t_c a, uint64_t val) {
    slot_set_uint(mutable_array_append(a), val);
}
static inline void mutable_array_append_float(mutable_array_t_c a, float val) {
    slot_set_float(mutable_array_append(a), val);
}
static inline void mutable_array_append_double(mutable_array_t_c a, double val) {
    slot_set_double(mutable_array_append(a), val);
}
static inline void mutable_array_append_string(mutable_array_t_c a, slice_t_c val) {
    slot_set_string(mutable_array_append(a), val);
}
static inline void mutable_array_append_data(mutable_array_t_c a, slice_t_c val) {
    slot_set_data(mutable_array_append(a), val);
}
static inline void mutable_array_append_value(mutable_array_t_c a, value_t_c val) {
    slot_set_value(mutable_array_append(a), val);
}
static inline void mutable_array_append_array(mutable_array_t_c a, array_t_c val) {
    slot_set_value(mutable_array_append(a), (value_t_c)val);
}
static inline void mutable_array_append_dict(mutable_array_t_c a, dict_t_c val) {
    slot_set_value(mutable_array_append(a), (value_t_c)val);
}
static inline void mutable_dict_set_null(mutable_dict_t_c d, slice_t_c key) {
    slot_set_null(mutable_dict_set(d, key));
}
static inline void mutable_dict_set_bool(mutable_dict_t_c d, slice_t_c key, bool val) {
    slot_set_bool(mutable_dict_set(d, key), val);
}
static inline void mutable_dict_set_int(mutable_dict_t_c d, slice_t_c key, int64_t val) {
    slot_set_int(mutable_dict_set(d, key), val);
}
static inline void mutable_dict_set_uint(mutable_dict_t_c d, slice_t_c key, uint64_t val) {
    slot_set_uint(mutable_dict_set(d, key), val);
}
static inline void mutable_dict_set_float(mutable_dict_t_c d, slice_t_c key, float val) {
    slot_set_float(mutable_dict_set(d, key), val);
}
static inline void mutable_dict_set_double(mutable_dict_t_c d, slice_t_c key, double val) {
    slot_set_double(mutable_dict_set(d, key), val);
}
static inline void mutable_dict_set_string(mutable_dict_t_c d, slice_t_c key, slice_t_c val) {
    slot_set_string(mutable_dict_set(d, key), val);
}
static inline void mutable_dict_set_data(mutable_dict_t_c d, slice_t_c key, slice_t_c val) {
    slot_set_data(mutable_dict_set(d, key), val);
}
static inline void mutable_dict_set_value(mutable_dict_t_c d, slice_t_c key, value_t_c val) {
    slot_set_value(mutable_dict_set(d, key), val);
}
static inline void mutable_dict_set_array(mutable_dict_t_c d, slice_t_c key, array_t_c val) {
    slot_set_value(mutable_dict_set(d, key), (value_t_c)val);
}
static inline void mutable_dict_set_dict(mutable_dict_t_c d, slice_t_c key, dict_t_c val) {
    slot_set_value(mutable_dict_set(d, key), (value_t_c)val);
}

}
