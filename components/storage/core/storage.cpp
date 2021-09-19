#include "mutable_array.h"
#include "mutable_dict.h"
#include "json_delta.hpp"
#include "storage.h"
#include "json5.hpp"
#include "better_assert.hpp"

#define CATCH_ERROR(out_error) \
    catch (const std::exception &x) { record_error(x, out_error); }

#define ENCODER_DO(E, METHOD) \
    (E->is_internal() ? E->encoder->METHOD : E->json_encoder->METHOD)

#define ENCODER_TRY(E, METHOD) \
    try{ \
    if (!E->has_error()) { \
    ENCODER_DO(E, METHOD); \
    return true; \
} \
} catch (const std::exception &x) { \
    E->record_exception(x); \
} \
    return false;


using namespace storage::impl;

namespace storage {

const value_t_c null_value_c  = storage::impl::value_t::null_value;
const array_t_c empty_array_c = storage::impl::array_t::empty_array;
const dict_t_c empty_dict_c   = storage::impl::dict_t::empty_dict;


static slice_result_t_c to_slice_result(alloc_slice_t &&s) {
    s.retain();
    return {(void*)s.buf, s.size};
}

value_t_c value_from_data(slice_t_c data, trust_type trust) noexcept {
    return trust == trust_type::trusted ? value_t::from_trusted_data(data) : value_t::from_data(data);
}

const char* dump(value_t_c v) noexcept {
    slice_result_t_c json = value_to_json(v);
    auto cstr = (char*)malloc(json.size + 1);
    memcpy(cstr, json.buf, json.size);
    cstr[json.size] = 0;
    return cstr;
}

const char* dump_data(slice_t_c data) noexcept {
    return dump(value_t::from_data(data));
}

value_type value_get_type(value_t_c v) noexcept {
    if (_usually_false(v == NULL))
        return value_type::undefined;
    auto type = v->type();
    if (_usually_false(type == value_type::null) && v->is_undefined())
        type = value_type::undefined;
    return type;
}

bool value_is_integer(value_t_c v) noexcept {
    return v && v->is_int();
}

bool value_is_unsigned(value_t_c v) noexcept {
    return v && v->is_unsigned();
}

bool value_is_double(value_t_c v) noexcept {
    return v && v->is_double();
}

bool value_as_bool(value_t_c v) noexcept {
    return v && v->as_bool();
}

int64_t value_as_int(value_t_c v) noexcept {
    return v ? v->as_int() : 0;
}

uint64_t value_as_unsigned(value_t_c v) noexcept {
    return v ? v->as_unsigned() : 0;
}

float value_as_float(value_t_c v) noexcept {
    return v ? v->as_float() : 0.0f;
}

double value_as_double(value_t_c v) noexcept {
    return v ? v->as_double() : 0.0;
}

slice_t_c value_as_string(value_t_c v) noexcept {
    return v ? (slice_t_c)v->as_string() : slice_null_c;
}

slice_t_c value_as_data(value_t_c v) noexcept {
    return v ? (slice_t_c)v->as_data() : slice_null_c;
}

array_t_c value_as_array(value_t_c v) noexcept {
    return v ? v->as_array() : nullptr;
}

dict_t_c value_as_dict(value_t_c v) noexcept {
    return v ? v->as_dict() : nullptr;
}

time_stamp value_as_timestamp(value_t_c v) noexcept {
    return v ? v->as_time_stamp() : time_stamp_none;
}

value_t_c value_retain(value_t_c v) noexcept {
    return retain(v);
}

void value_release(value_t_c v) noexcept {
    release(v);
}

bool value_is_mutable(value_t_c v) noexcept {
    return v && v->is_mutable();
}

doc_t_c value_find_doc(value_t_c v) noexcept {
    return v ? retain(doc_t::containing(v).get()) : nullptr;
}

bool value_is_equals(value_t_c v1, value_t_c v2) noexcept {
    if (_usually_true(v1 != nullptr))
        return v1->is_equal(v2);
    else
        return v2 == nullptr;
}

slice_result_t_c value_to_string(value_t_c v) noexcept {
    if (v) {
        try {
            return to_slice_result(v->to_string());
        } CATCH_ERROR(nullptr);
    }
    return {nullptr, 0};
}

value_t_c value_new_string(slice_t_c str) noexcept {
    try {
        return retain(internal::heap_value_t::create(str))->as_value();
    } CATCH_ERROR(nullptr);
    return nullptr;
}

value_t_c value_new_data(slice_t_c data) noexcept {
    try {
        return retain(internal::heap_value_t::create_data(data))->as_value();
    } CATCH_ERROR(nullptr);
    return nullptr;
}

slice_result_t_c value_to_jsonx(value_t_c v, bool json5, bool canonical_form) noexcept
{
    if (v) {
        try {
            json_encoder_t encoder;
            encoder.set_json5(json5);
            encoder.set_canonical(canonical_form);
            encoder.write_value(v);
            return to_slice_result(encoder.finish());
        } CATCH_ERROR(nullptr);
    }
    return {nullptr, 0};
}

slice_result_t_c value_to_json(value_t_c v) noexcept {
    return value_to_jsonx(v, false, false);
}

slice_result_t_c value_to_json5(value_t_c v) noexcept {
    return value_to_jsonx(v, true,  false);
}

slice_result_t_c data_convert_json(slice_t_c json, error_code *out_error) noexcept {
    encoder_impl_t e(encode_format::internal, json.size);
    encoder_convert_json(&e, json);
    return encoder_finish(&e, out_error);
}

slice_result_t_c json5_to_json(slice_t_c json5, slice_result_t_c *out_error_message, size_t *out_error_pos, error_code *out_error) noexcept {
    alloc_slice_t error_message;
    size_t error_pos = 0;
    try {
        std::string json = convert_json5((std::string((char*)json5.buf, json5.size)));
        return to_slice_result(alloc_slice_t(json));
    } catch (const json5_error &x) {
        error_message = alloc_slice_t(x.what());
        error_pos = x.input_pos;
        if (out_error)
            *out_error = error_code::json_error;
    } catch (const std::exception &x) {
        error_message = alloc_slice_t(x.what());
        record_error(x, out_error);
    }
    if (out_error_message)
        *out_error_message = to_slice_result(std::move(error_message));
    if (out_error_pos)
        *out_error_pos = error_pos;
    return {};
}

slice_result_t_c data_dump(slice_t_c data) noexcept {
    try {
        return to_slice_result(alloc_slice_t(value_t::dump(data)));
    } CATCH_ERROR(nullptr);
    return {nullptr, 0};
}

uint32_t array_count(array_t_c a) noexcept {
    return a ? a->count() : 0;
}

bool array_is_empty(array_t_c a) noexcept {
    return a ? a->empty() : true;
}

value_t_c array_get(array_t_c a, uint32_t index) noexcept {
    return a ? a->get(index) : nullptr;
}

void array_iterator_begin(array_t_c a, array_iterator_t_c* i) noexcept {
    static_assert(sizeof(array_iterator_t_c) >= sizeof(array_t::iterator), "array_iterator_t_c is too small");
    new (i) array_t::iterator(a);
}

uint32_t array_iterator_get_count(const array_iterator_t_c* i) noexcept {
    return ((array_t::iterator*)i)->count();
}

value_t_c array_iterator_get_value(const array_iterator_t_c* i) noexcept {
    return ((array_t::iterator*)i)->value();
}

value_t_c array_iterator_get_value_at(const array_iterator_t_c *i, uint32_t offset) noexcept {
    return (*(array_t::iterator*)i)[offset];
}

bool array_iterator_next(array_iterator_t_c* i) noexcept {
    try {
        auto& iter = *(array_t::iterator*)i;
        ++iter;
        return (bool)iter;
    } CATCH_ERROR(nullptr);
    return false;
}

static mutable_array_t_c _new_mutable_array(array_t_c a, copy_flags flags) noexcept {
    try {
        return (mutable_array_t*)retain(mutable_array_t::new_array(a, copy_flags(flags)));
    } CATCH_ERROR(nullptr);
    return nullptr;
}

mutable_array_t_c mutable_array_new(void) noexcept {
    return _new_mutable_array(nullptr, copy_flags::deep_copy);
}

mutable_array_t_c array_mutable_copy(array_t_c a, copy_flags flags) noexcept {
    return a ? _new_mutable_array(a, flags) : nullptr;
}

mutable_array_t_c array_as_mutable(array_t_c a) noexcept {
    return a ? a->as_mutable() : nullptr;
}

array_t_c mutable_array_get_source(mutable_array_t_c a) noexcept {
    return a ? a->source() : nullptr;
}

bool mutable_array_is_changed(mutable_array_t_c a) noexcept {
    return a && a->is_changed();
}

void mutable_array_set_changed(mutable_array_t_c a, bool c) noexcept {
    if (a) a->set_changed(c);
}

void mutable_array_resize(mutable_array_t_c a, uint32_t size) noexcept {
    a->resize(size);
}

slot_t_c mutable_array_set(mutable_array_t_c array, uint32_t index) noexcept {
    return &array->setting(index);
}

slot_t_c mutable_array_append(mutable_array_t_c a) noexcept {
    return &a->appending();
}

void mutable_array_insert(mutable_array_t_c a, uint32_t first_index, uint32_t count) noexcept {
    if (a) a->insert(first_index, count);
}

void mutable_array_remove(mutable_array_t_c a, uint32_t first_index, uint32_t count) noexcept {
    if(a) a->remove(first_index, count);
}

mutable_array_t_c mutable_array_get_mutable_array(mutable_array_t_c a, uint32_t index) noexcept {
    return a ? a->get_mutable_array(index) : nullptr;
}

mutable_dict_t_c mutable_array_get_mutable_dict(mutable_array_t_c a, uint32_t index) noexcept {
    return a ? a->get_mutable_dict(index) : nullptr;
}

uint32_t dict_count(dict_t_c d)  noexcept {
    return d ? d->count() : 0;
}

bool dict_is_empty(dict_t_c d) noexcept {
    return d ? d->empty() : true;
}

value_t_c dict_get(dict_t_c d, slice_t_c key_string) noexcept {
    return d ? d->get(key_string) : nullptr;
}

void dict_iterator_begin(dict_t_c d, dict_iterator_t_c* i) noexcept {
    static_assert(sizeof(dict_iterator_t_c) >= sizeof(dict_t::iterator), "dict_iterator_t_c is too small");
    new (i) dict_t::iterator(d);
}

value_t_c dict_iterator_get_key(const dict_iterator_t_c* i) noexcept {
    return ((dict_t::iterator*)i)->key();
}

slice_t_c dict_iterator_get_key_string(const dict_iterator_t_c* i) noexcept {
    return ((dict_t::iterator*)i)->key_string();
}

value_t_c dict_iterator_get_value(const dict_iterator_t_c* i) noexcept {
    return ((dict_t::iterator*)i)->value();
}

uint32_t dict_iterator_get_count(const dict_iterator_t_c* i) noexcept {
    return ((dict_t::iterator*)i)->count();
}

bool dict_iterator_next(dict_iterator_t_c* i) noexcept {
    try {
        auto& iter = *(dict_t::iterator*)i;
        ++iter;
        if (iter)
            return true;
        iter.~dict_iterator_t();
    } CATCH_ERROR(nullptr);
    return false;
}

void dict_iterator_end(dict_iterator_t_c* i) noexcept {
    ((dict_t::iterator*)i)->~dict_iterator_t();
}

dict_key_t_c dict_key_init(slice_t_c string) noexcept {
    dict_key_t_c key;
    static_assert(sizeof(dict_key_t_c) >= sizeof(dict_t::key_t), "dict_key_t_c is too small");
    new (&key) dict_t::key_t(string);
    return key;
}

slice_t_c dict_key_get_string(const dict_key_t_c *key) noexcept {
    auto real_key = (const dict_t::key_t*)key;
    return real_key->string();
}

value_t_c dict_get_with_key(dict_t_c d, dict_key_t_c *k) noexcept {
    if (!d)
        return nullptr;
    auto &key = *(dict_t::key_t*)k;
    return d->get(key);
}

static mutable_dict_t_c _new_mutable_dict(dict_t_c d, copy_flags flags) noexcept {
    try {
        return (mutable_dict_t*)retain(mutable_dict_t::new_dict(d, copy_flags(flags)));
    } CATCH_ERROR(nullptr);
    return nullptr;
}

mutable_dict_t_c mutable_dict_new(void) noexcept {
    return _new_mutable_dict(nullptr, copy_flags::default_copy);
}

mutable_dict_t_c dict_mutable_copy(dict_t_c d, copy_flags flags) noexcept {
    return d ? _new_mutable_dict(d, flags) : nullptr;
}

mutable_dict_t_c dict_as_mutable(dict_t_c d) noexcept {
    return d ? d->as_mutable() : nullptr;
}

dict_t_c mutable_dict_get_source(mutable_dict_t_c d) noexcept {
    return d ? d->source() : nullptr;
}

bool mutable_dict_is_changed(mutable_dict_t_c d) noexcept {
    return d && d->is_changed();
}

void mutable_dict_set_changed(mutable_dict_t_c d, bool changed) noexcept {
    if (d) d->set_changed(changed);
}

slot_t_c mutable_dict_set(mutable_dict_t_c dict, slice_t_c k) noexcept {
    return &dict->setting(k);
}

void mutable_dict_remove(mutable_dict_t_c d, slice_t_c key) noexcept {
    if(d) d->remove(key);
}

void mutable_dict_remove_all(mutable_dict_t_c d) noexcept {
    if(d) d->remove_all();
}

mutable_array_t_c mutable_dict_get_mutable_array(mutable_dict_t_c d, slice_t_c key) noexcept {
    return d ? d->get_mutable_array(key) : nullptr;
}

mutable_dict_t_c mutable_dict_get_mutable_dict(mutable_dict_t_c d, slice_t_c key) noexcept {
    return d ? d->get_mutable_dict(key) : nullptr;
}

shared_keys_t_c shared_keys_new() noexcept {
    return retain(new shared_keys_t());
}

shared_keys_t_c shared_keys_retain(shared_keys_t_c sk) noexcept {
    return retain(sk);
}

void shared_keys_release(shared_keys_t_c sk) noexcept {
    release(sk);
}

unsigned shared_keys_count(shared_keys_t_c sk) noexcept {
    return (unsigned)sk->count();
}

bool shared_keys_load_state_data(shared_keys_t_c sk, slice_t_c d) noexcept {
    return sk->load_from(d);
}

bool shared_keys_load_state(shared_keys_t_c sk, value_t_c s) noexcept {
    return sk->load_from(s);
}

slice_result_t_c shared_keys_get_state_data(shared_keys_t_c sk) noexcept {
    return to_slice_result(sk->state_data());
}

slice_t_c shared_keys_decode(shared_keys_t_c sk, int key) noexcept {
    return sk->decode(key);
}

void shared_keys_revert_to_count(shared_keys_t_c sk, unsigned old_count) noexcept {
    sk->revert_to_count(old_count);
}

void shared_keys_write_state(shared_keys_t_c sk, encoder_t_c e) noexcept {
    assert_always(e->is_internal());
    sk->write_state(*e->encoder);
}

int shared_keys_encode(shared_keys_t_c sk, slice_t_c key_str, bool add) noexcept {
    int int_key;
    if (!(add ? sk->encode_and_add(key_str, int_key) : sk->encode(key_str, int_key)))
        int_key = -1;
    return int_key;
}

shared_key_scope shared_key_scope_with_range(slice_t_c range, shared_keys_t_c sk) noexcept {
    return (shared_key_scope) new scope_t(range, sk);
}

void shared_key_scope_free(shared_key_scope scope) {
    delete (scope_t*) scope;
}

shared_keys_t_c shared_keys_create() noexcept {
    return shared_keys_new();
}

shared_keys_t_c shared_keys_create_from_state_data(slice_t_c data) noexcept {
    shared_keys_t_c keys = shared_keys_new();
    if (keys)
        shared_keys_load_state_data(keys, data);
    return keys;
}

void slot_set_null(slot_t_c slot) noexcept {
    slot->set(null_value_t());
}

void slot_set_bool(slot_t_c slot, bool v) noexcept {
    slot->set(v);
}

void slot_set_int(slot_t_c slot, int64_t v) noexcept {
    slot->set(v);
}

void slot_set_uint(slot_t_c slot, uint64_t v) noexcept {
    slot->set(v);
}

void slot_set_float(slot_t_c slot, float v)noexcept {
    slot->set(v);
}

void slot_set_double(slot_t_c slot, double v) noexcept {
    slot->set(v);
}

void slot_set_string(slot_t_c slot, slice_t_c v) noexcept {
    slot->set(v);
}

void slot_set_data(slot_t_c slot, slice_t_c v) noexcept {
    slot->set_data(v);
}

void slot_set_value(slot_t_c slot, value_t_c v) noexcept {
    slot->set(v);
}

deep_iterator_t_c deep_iterator_new(value_t_c v) noexcept {
    return new deep_iterator_t(v);
}

void deep_iterator_free(deep_iterator_t_c i) noexcept {
    delete i;
}

value_t_c deep_iterator_get_value(deep_iterator_t_c i) noexcept {
    return i->value();
}

value_t_c deep_iterator_get_parent(deep_iterator_t_c i) noexcept {
    return i->parent();
}

slice_t_c deep_iterator_get_key(deep_iterator_t_c i) noexcept {
    return i->key_string();
}

uint32_t deep_iterator_get_index(deep_iterator_t_c i) noexcept {
    return i->index();
}

size_t deep_iterator_get_depth(deep_iterator_t_c i) noexcept {
    return i->path().size();
}

void deep_iterator_skip_children(deep_iterator_t_c i) noexcept {
    i->skip_children();
}

bool deep_iterator_next(deep_iterator_t_c i) noexcept {
    i->next();
    return i->value() != nullptr;
}

void deep_iterator_get_path(deep_iterator_t_c i, path_component_t_c* *out_path, size_t *out_depth) noexcept {
    static_assert(sizeof(path_component_t_c) == sizeof(deep_iterator_t::path_component_t),
                  "path_component_t_c does not match path_component_t");
    auto &path = i->path();
    *out_path = (path_component_t_c*) path.data();
    *out_depth = path.size();
}

slice_result_t_c deep_iterator_get_path_string(deep_iterator_t_c i) noexcept {
    return to_slice_result(alloc_slice_t(i->path_string()));
}

slice_result_t_c deep_iterator_get_json_pointer(deep_iterator_t_c i) noexcept {
    return to_slice_result(alloc_slice_t(i->json_pointer()));
}

key_path_t_c key_path_new(slice_t_c specifier, error_code *out_error) noexcept {
    try {
        return new path_t((std::string)(slice_t)specifier);
    } CATCH_ERROR(out_error);
    return nullptr;
}

void key_path_free(key_path_t_c path) noexcept {
    delete path;
}

value_t_c key_path_eval(key_path_t_c path, value_t_c root) noexcept {
    return path->eval(root);
}

value_t_c key_path_eval_once(slice_t_c specifier, value_t_c root, error_code *out_error) noexcept {
    try {
        return path_t::eval((std::string)(slice_t)specifier, root);
    } CATCH_ERROR(out_error);
    return nullptr;
}

slice_result_t_c key_path_to_string(key_path_t_c path) noexcept {
    return to_slice_result(alloc_slice_t(std::string(*path)));
}

bool key_path_equals(key_path_t_c path1, key_path_t_c path2) noexcept {
    return *path1 == *path2;
}

bool key_path_get_element(key_path_t_c path, size_t i, slice_t_c *out_key, int32_t *out_index) noexcept {
    if (i >= path->size())
        return false;
    auto &element = (*path)[i];
    *out_key = element.key_str();
    *out_index = element.index();
    return true;
}

encoder_t_c encoder_new(void) noexcept {
    return encoder_new_with_options(encode_format::internal, 0, true);
}

encoder_t_c encoder_new_with_options(encode_format format, size_t reserve_size, bool unique_strings) noexcept {
    return new encoder_impl_t(format, reserve_size, unique_strings);
}

encoder_t_c encoder_new_writing_to_file(FILE *output_file, bool unique_strings) noexcept {
    return new encoder_impl_t(output_file, unique_strings);
}

void encoder_reset(encoder_t_c e) noexcept {
    e->reset();
}

void encoder_free(encoder_t_c encodere) noexcept {
    delete encodere;
}

void encoder_set_shared_keys(encoder_t_c e, shared_keys_t_c sk) noexcept {
    if (e->is_internal())
        e->encoder->set_shared_keys(sk);
}

void encoder_suppress_trailer(encoder_t_c e) noexcept {
    if (e->is_internal())
        e->encoder->suppress_trailer();
}

void encoder_amend(encoder_t_c e, slice_t_c base, bool reuse_strings, bool extern_pointers) noexcept {
    if (e->is_internal() && base.size > 0) {
        e->encoder->set_base(base, extern_pointers);
        if(reuse_strings)
            e->encoder->reuse_base_strings();
    }
}

slice_t_c encoder_get_base(encoder_t_c e) noexcept {
    if (e->is_internal())
        return e->encoder->base();
    return {};
}

size_t encoder_get_next_write_pos(encoder_t_c e) noexcept {
    if (e->is_internal())
        return e->encoder->next_write_pos();
    return 0;
}

size_t encoder_bytes_written(encoder_t_c e) noexcept {
    return ENCODER_DO(e, bytes_written_size());
}

intptr_t encoder_last_value_written(encoder_t_c e) {
    if (e->is_internal())
        return intptr_t(e->encoder->last_value_written());
    return 0;
}

void encoder_write_value_again(encoder_t_c e, intptr_t pre_written_value) {
    if (e->is_internal())
        e->encoder->write_value_again(encoder_t::pre_written_value(pre_written_value));
}

bool encoder_write_null(encoder_t_c e) noexcept {
    ENCODER_TRY(e, write_null());
}

bool encoder_write_undefined(encoder_t_c e) noexcept {
    ENCODER_TRY(e, write_undefined());
}

bool encoder_write_bool(encoder_t_c e, bool b) noexcept {
    ENCODER_TRY(e, write_bool(b));
}

bool encoder_write_int(encoder_t_c e, int64_t i) noexcept {
    ENCODER_TRY(e, write_int(i));
}

bool encoder_write_uint(encoder_t_c e, uint64_t u) noexcept {
    ENCODER_TRY(e, write_uint(u));
}

bool encoder_write_float(encoder_t_c e, float f) noexcept {
    ENCODER_TRY(e, write_float(f));
}

bool encoder_write_double(encoder_t_c e, double d) noexcept {
    ENCODER_TRY(e, write_double(d));
}

bool encoder_write_string(encoder_t_c e, slice_t_c s) noexcept {
    ENCODER_TRY(e, write_string(s));
}

bool encoder_write_date_string(encoder_t_c e, time_stamp ts, bool utc) noexcept {
    ENCODER_TRY(e, write_date_string(ts,utc));
}

bool encoder_write_data(encoder_t_c e, slice_t_c d) noexcept {
    ENCODER_TRY(e, write_data(d));
}

bool encoder_write_raw(encoder_t_c e, slice_t_c r) noexcept {
    ENCODER_TRY(e, write_raw(r));
}

bool encoder_write_value(encoder_t_c e, value_t_c v) noexcept {
    ENCODER_TRY(e, write_value(v));
}

bool encoder_begin_array(encoder_t_c e, size_t reserve_count) noexcept {
    ENCODER_TRY(e, begin_array(reserve_count));
}

bool encoder_end_array(encoder_t_c e) noexcept {
    ENCODER_TRY(e, end_array());
}

bool encoder_begin_dict(encoder_t_c e, size_t reserve_count) noexcept {
    ENCODER_TRY(e, begin_dict(reserve_count));
}

bool encoder_write_key(encoder_t_c e, slice_t_c s) noexcept {
    ENCODER_TRY(e, write_key(s));
}

bool encoder_write_key_value(encoder_t_c e, value_t_c key) noexcept {
    ENCODER_TRY(e, write_key(key));
}

bool encoder_end_dict(encoder_t_c e) noexcept {
    ENCODER_TRY(e, end_dict());
}

bool encoder_convert_json(encoder_t_c e, slice_t_c json) noexcept {
    if (!e->has_error()) {
        try {
            if (e->is_internal()) {
                json_converter_t *jc = e->json_converter.get();
                if (jc) {
                    jc->reset();
                } else {
                    jc = new json_converter_t(*e->encoder);
                    e->json_converter.reset(jc);
                }
                if (jc->encode_json(json)) {
                    return true;
                } else {
                    e->error = (error_code)jc->error();
                    e->error_message = jc->error_message();
                }
            } else {
                e->json_encoder->write_json(json);
                return true;
            }
        } catch (const std::exception &x) {
            e->record_exception(x);
        }
    }
    return false;
}

error_code encoder_get_error(encoder_t_c e) noexcept {
    return (error_code)e->error;
}

const char* encoder_get_error_message(encoder_t_c e) noexcept {
    return e->has_error() ? e->error_message.c_str() : nullptr;
}

void encoder_set_extra_info(encoder_t_c e, void *info) noexcept {
    e->extra_info = info;
}

void* encoder_get_extra_info(encoder_t_c e) noexcept {
    return e->extra_info;
}

slice_result_t_c encoder_snip(encoder_t_c e) {
    if (e->is_internal())
        return slice_result_t_c(e->encoder->snip());
    else
        return {};
}

size_t encoder_finish_item(encoder_t_c e) noexcept {
    if (e->is_internal())
        return e->encoder->finish_item();
    return 0;
}

doc_t_c encoder_finish_doc(encoder_t_c e, error_code *out_error) noexcept {
    if (e->encoder) {
        if (!e->has_error()) {
            try {
                return retain(e->encoder->finish_doc());
            } catch (const std::exception &x) {
                e->record_exception(x);
            }
        }
    } else {
        e->error = error_code::unsupported;
    }
    if (out_error)
        *out_error = e->error;
    e->reset();
    return nullptr;
}

slice_result_t_c encoder_finish(encoder_t_c e, error_code *out_error) noexcept {
    if (!e->has_error()) {
        try {
            return to_slice_result(ENCODER_DO(e, finish()));
        } catch (const std::exception &x) {
            e->record_exception(x);
        }
    }
    if (out_error)
        *out_error = e->error;
    e->reset();
    return {nullptr, 0};
}

doc_t_c doc_from_result_data(slice_result_t_c data, trust_type trust, shared_keys_t_c sk, slice_t_c extern_data) noexcept {
    return retain(new doc_t(alloc_slice_t(data), (doc_t::trust_type)trust, sk, extern_data));
}

doc_t_c doc_from_json(slice_t_c json, error_code *out_error) noexcept {
    try {
        return retain(doc_t::from_json(json));
    } CATCH_ERROR(out_error);
    return nullptr;
}

void doc_release(doc_t_c doc) noexcept {
    release(doc);
}

doc_t_c doc_retain(doc_t_c doc) noexcept {
    return retain(doc);
}

shared_keys_t_c doc_get_shared_keys(doc_t_c doc) noexcept {
    return doc ? doc->shared_keys() : nullptr;
}

value_t_c doc_get_root(doc_t_c doc) noexcept {
    return doc ? doc->root() : nullptr;
}

slice_t_c doc_get_data(doc_t_c doc) noexcept {
    return doc ? doc->data() : slice_t();
}

slice_result_t_c doc_get_alloced_data(doc_t_c doc) noexcept {
    return doc ? to_slice_result(doc->alloced_data()) : slice_result_t_c{};
}

void* doc_get_associated(doc_t_c doc, const char *type) noexcept {
    return doc ? doc->get_associated(type) : nullptr;
}

bool doc_set_associated(doc_t_c doc, void *pointer, const char *type) noexcept {
    return doc && const_cast<doc_t*>(doc)->set_associated(pointer, type);
}

slice_result_t_c create_json_delta(value_t_c old, value_t_c nuu) noexcept {
    try {
        return to_slice_result(json_delta_t::create(old, nuu));
    } catch (const std::exception&) {
        return {};
    }
}

bool encode_json_delta(value_t_c old, value_t_c nuu, encoder_t_c json_encoder) noexcept {
    try {
        json_encoder_t *enc = json_encoder->json_encoder.get();
        precondition(enc);
        json_delta_t::create(old, nuu, *enc);
        return true;
    } catch (const std::exception &x) {
        json_encoder->record_exception(x);
        return false;
    }
}

slice_result_t_c apply_json_delta(value_t_c old, slice_t_c json_delta, error_code *out_error) noexcept {
    try {
        return to_slice_result(json_delta_t::apply(old, json_delta));
    } CATCH_ERROR(out_error);
    return {};
}

bool encode_applying_json_delta(value_t_c old, slice_t_c json_delta, encoder_t_c encoder) noexcept {
    try {
        encoder_t *enc = encoder->encoder.get();
        if (!enc)
            exception_t::_throw(error_code::encode_error, "encode_applying_json_delta cannot encode JSON");
        json_delta_t::apply(old, json_delta, false, *enc);
        return true;
    } catch (const std::exception &x) {
        encoder->record_exception(x);
        return false;
    }
}

}
