#include "storage.hpp"
#include "mutable_array.h"
#include "mutable_dict.h"
#include "json_delta.hpp"
#include "better_assert.hpp"

#define CATCH_ERROR(out_error) \
    catch (const std::exception &x) { storage::impl::record_error(x, out_error); }

#define ENCODER_DO(E, METHOD) \
    E->is_internal() ? E->encoder->METHOD : E->json_encoder->METHOD

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

namespace storage {

const impl_value_t null_value  = storage::impl::value_t::null_value;
const impl_array_t empty_array = storage::impl::array_t::empty_array;
const impl_dict_t empty_dict   = storage::impl::dict_t::empty_dict;


value_t::value_t(impl_value_t v)
    : _val(v)
{}

value_t value_t::null() {
    return value_t(null_value);
}

std::string value_t::to_json_string() const {
    return std::string(to_json());
}

std::string value_t::to_std_string() const {
    return as_string().as_string();
}

value_t::operator bool() const {
    return _val != nullptr;
}

bool value_t::operator!() const {
    return _val == nullptr;
}

bool value_t::operator==(value_t v) const {
    return _val == v._val;
}

bool value_t::operator==(impl_value_t v) const {
    return _val == v;
}

bool value_t::operator!=(value_t v) const {
    return _val != v._val;
}

bool value_t::operator!=(impl_value_t v) const {
    return _val != v;
}

bool value_t::is_equals(value_t v) const {
    if (_val == nullptr)
        return v._val == nullptr;
    return _val->is_equal(v._val);
}

value_t &value_t::operator=(value_t v) {
    _val = v._val;
    return *this;
}

value_t &value_t::operator=(std::nullptr_t) {
    _val = nullptr; return *this;
}

value_t value_t::from_data(slice_t data, trust_type trust) {
    return trust == trust_type::trusted ? impl::value_t::from_trusted_data(data) : impl::value_t::from_data(data);
}

storage::value_t::operator impl_value_t() const {
    return _val;
}

value_type value_t::type() const {
    if (_val == nullptr) return value_type::undefined;
    auto type = _val->type();
    if (type == value_type::null && _val->is_undefined()) type = value_type::undefined;
    return type;
}

bool value_t::is_integer() const {
    return _val && _val->is_int();
}

bool value_t::is_unsigned() const {
    return _val && _val->is_unsigned();
}

bool value_t::is_double() const {
    return _val && _val->is_double();
}

bool value_t::is_mutable() const {
    return _val && _val->is_mutable();
}

bool value_t::as_bool() const {
    return _val && _val->as_bool();
}

int64_t value_t::as_int() const {
    return _val ? _val->as_int() : 0;
}

uint64_t value_t::as_unsigned() const {
    return _val ? _val->as_unsigned() : 0;
}

float value_t::as_float() const {
    return _val ? _val->as_float() : 0.0f;
}

double value_t::as_double() const {
    return _val ? _val->as_double() : 0.0;
}

time_stamp value_t::as_time_stamp() const {
    return _val ? _val->as_time_stamp() : time_stamp_none;
}

slice_t value_t::as_string() const {
    return _val ? static_cast<slice_t_c>(_val->as_string()) : slice_null_c;
}

slice_t value_t::as_data() const {
    return _val ? static_cast<slice_t_c>(_val->as_data()) : slice_null_c;
}

array_t value_t::as_array() const {
    return _val ? _val->as_array() : nullptr;
}

dict_t value_t::as_dict() const {
    return _val ? _val->as_dict() : nullptr;
}

alloc_slice_t value_t::to_string() const {
    return _val ? _val->to_string() : alloc_slice_t();
}

alloc_slice_t value_t::to_json(bool canonical) const {
    return _val ? _val->to_json(canonical) : alloc_slice_t();
}

value_t value_t::operator[] (const key_path_t &kp) const {
    return kp._path->eval(_val);
}

doc_t value_t::find_doc() const {
    return doc_t(_val);
}


array_t::iterator_t::iterator_t(array_t array)
    : array_iterator_t(static_cast<impl_array_t>(array))
{}

array_t::iterator_t::iterator_t(const array_iterator_t &i)
    : array_iterator_t(i)
{}

value_t array_t::iterator_t::value() const {
    return array_iterator_t::value();
}

uint32_t array_t::iterator_t::count() const {
    return array_iterator_t::count();
}

bool array_t::iterator_t::next() {
    auto& iter = *static_cast<array_iterator_t*>(this);
    ++iter;
    return (bool)iter;
}

impl_value_t array_t::iterator_t::operator ->() const {
    return impl_value_t(value());
}

value_t array_t::iterator_t::operator *() const {
    return value();
}

storage::array_t::iterator_t::operator bool() const {
    return (bool)value();
}

array_t::iterator_t &array_t::iterator_t::operator++() {
    next();
    return *this;
}

bool array_t::iterator_t::operator!=(const array_t::iterator_t &) {
    return value() != nullptr;
}

value_t array_t::iterator_t::operator[](unsigned n) const {
    return array_iterator_t::operator[](n);
}


array_t::array_t()
    : value_t()
{}

array_t::array_t(impl_array_t a)
    : value_t(a)
{}

array_t::operator impl_array_t() const {
    return static_cast<impl_array_t>(_val);
}

array_t array_t::empty_array() {
    return array_t(storage::empty_array);
}

value_t array_t::operator[](int index) const {
    return get(index);
}

value_t array_t::operator[](const key_path_t &kp) const {
    return value_t::operator[](kp);
}

array_t &array_t::operator=(array_t a) {
    _val = a._val;
    return *this;
}

array_t &array_t::operator=(std::nullptr_t) {
    _val = nullptr;
    return *this;
}

array_t::iterator_t array_t::begin() const {
    return iterator_t(*this);
}

array_t::iterator_t array_t::end() const {
    return iterator_t();
}

uint32_t array_t::count() const {
    return _val ? static_cast<impl_array_t>(_val)->count() : 0;
}

bool array_t::empty() const {
    return _val ? static_cast<impl_array_t>(_val)->count() : true;
}

value_t array_t::get(uint32_t index) const {
    return _val ? static_cast<impl_array_t>(_val)->get(index) : nullptr;
}


dict_t::key_t::key_t(nonnull_slice string)
    : key_t(alloc_slice_t(string))
{}

dict_t::key_t::key_t(alloc_slice_t string)
    : _str(std::move(string))
    , _key(impl::key_t(_str))
{
}

const alloc_slice_t &dict_t::key_t::string() const {
    return _str;
}

storage::dict_t::key_t::operator const alloc_slice_t &() const {
    return _str;
}

storage::dict_t::key_t::operator nonnull_slice() const {
    return _str;
}


dict_t::iterator_t::iterator_t(dict_t dict) {
    dict_iterator_t(static_cast<impl_dict_t>(dict));
}

//delete
//dict_t::iterator_t::iterator_t(const dict_iterator_t &i)
//    : dict_iterator_t(i)
//{}

uint32_t dict_t::iterator_t::count() const {
    return dict_iterator_t::count();
}

value_t dict_t::iterator_t::key() const {
    return dict_iterator_t::key();
}

slice_t dict_t::iterator_t::key_string() const {
    return dict_iterator_t::key_string();
}

value_t dict_t::iterator_t::value() const {
    return dict_iterator_t::value();
}

bool dict_t::iterator_t::next() {
    auto& iter = *static_cast<dict_iterator_t*>(this);
    ++iter;
    return (bool)iter;
}

impl_value_t dict_t::iterator_t::operator ->() const {
    return impl_value_t(value());
}

value_t dict_t::iterator_t::operator *() const {
    return value();
}

storage::dict_t::iterator_t::operator bool() const {
    return (bool)value();
}

dict_t::iterator_t &dict_t::iterator_t::operator++() {
    next(); return *this;
}

bool dict_t::iterator_t::operator!=(const dict_t::iterator_t &) const {
    return value() != nullptr;
}


dict_t::dict_t()
    : value_t()
{}

dict_t::dict_t(impl_dict_t d)
    : value_t(d)
{}

storage::dict_t::operator impl_dict_t() const {
    return static_cast<impl_dict_t>(_val);
}

dict_t dict_t::empty_dict() {
    return dict_t(storage::empty_dict);
}

value_t dict_t::get(const char *key) const {
    return get(slice_t(key));
}

value_t dict_t::operator[](nonnull_slice key) const {
    return get(key);
}

value_t dict_t::operator[](const char *key) const {
    return get(key);
}

value_t dict_t::operator[](const key_path_t &kp) const {
    return value_t::operator[](kp);
}

dict_t &dict_t::operator=(dict_t d) {
    _val = d._val; return *this;
}

dict_t &dict_t::operator=(std::nullptr_t) {
    _val = nullptr; return *this;
}

value_t dict_t::operator[](dict_t::key_t &key) const {
    return get(key);
}

dict_t::iterator_t dict_t::begin() const {
    return iterator_t(*this);
}

dict_t::iterator_t dict_t::end() const {
    return iterator_t();
}

uint32_t dict_t::count() const {
    return _val ? static_cast<impl_dict_t>(_val)->count() : 0;
}

bool dict_t::empty() const {
    return _val ? static_cast<impl_dict_t>(_val)->empty() : true;
}

value_t dict_t::get(nonnull_slice key) const {
    return _val ? static_cast<impl_dict_t>(_val)->get(key) : nullptr;
}

value_t dict_t::get(dict_t::key_t &key) const {
    return get(static_cast<nonnull_slice>(key));
}


key_path_t::key_path_t(nonnull_slice spec, error_code *error) {
    try {
        _path = new impl::path_t(spec);
    } CATCH_ERROR(error);
}

key_path_t::key_path_t(key_path_t &&kp)
    : _path(kp._path) {
    kp._path = nullptr;
}

key_path_t &key_path_t::operator=(key_path_t &&kp) {
    delete _path;
    _path = kp._path;
    kp._path = nullptr;
    return *this;
}

key_path_t::key_path_t(const key_path_t &kp)
    : key_path_t(std::string(kp), nullptr)
{}

key_path_t::~key_path_t() {
    delete _path;
}

storage::key_path_t::operator bool() const {
    return _path != nullptr;
}

storage::key_path_t::operator impl_key_path_t() const {
    return _path;
}

value_t key_path_t::eval(value_t root) const {
    return _path->eval(root);
}

value_t key_path_t::eval(nonnull_slice specifier, value_t root, error_code *error) {
    try {
        auto key = key_path_t(specifier, error);
        return key.eval(root);
    } CATCH_ERROR(error);
    return nullptr;
}

key_path_t::operator std::string() const {
    return _path->operator std::string();
}

bool key_path_t::operator==(const key_path_t &kp) const {
    if (_path == nullptr) return kp._path == nullptr;
    return *_path == *kp._path;
}


deep_iterator_t::deep_iterator_t(value_t v)
    : _i(new impl::deep_iterator_t(static_cast<impl_value_t>(v)))
{}

deep_iterator_t::~deep_iterator_t() {
    delete _i;
}

value_t deep_iterator_t::value() const {
    return _i->value();
}

slice_t deep_iterator_t::key() const {
    return _i->key_string();
}

uint32_t deep_iterator_t::index() const {
    return _i->index();
}

value_t deep_iterator_t::parent() const {
    return _i->parent();
}

size_t deep_iterator_t::depth() const {
    return _i->path().size();
}

alloc_slice_t deep_iterator_t::path_string() const {
    return alloc_slice_t(_i->path_string());
}

alloc_slice_t deep_iterator_t::json_pointer() const {
    return alloc_slice_t(_i->json_pointer());
}

void deep_iterator_t::skip_children() {
    _i->skip_children();
}

bool deep_iterator_t::next() {
    _i->next();
    return _i->value() != nullptr;
}

storage::deep_iterator_t::operator bool() const {
    return value() != nullptr;
}

deep_iterator_t &deep_iterator_t::operator++() {
    next();
    return *this;
}


shared_keys_t::shared_keys_t()
    : _sk(nullptr)
{}

shared_keys_t::shared_keys_t(impl_shared_keys_t sk)
    : _sk(retain(sk))
{}

shared_keys_t::~shared_keys_t() {
    release(_sk);
}

shared_keys_t shared_keys_t::create() {
    return shared_keys_t(retain(new impl::shared_keys_t()), 1);
}

bool shared_keys_t::load_state(slice_t data) {
    return _sk->load_from(data);
}

bool shared_keys_t::load_state(value_t state) {
    return _sk->load_from(state);
}

alloc_slice_t shared_keys_t::state_data() const {
    return _sk->state_data();
}

unsigned shared_keys_t::count() const {
    return static_cast<unsigned>(_sk->count());
}

void shared_keys_t::revert_to_count(unsigned count) {
    _sk->revert_to_count(count);
}

storage::shared_keys_t::operator impl_shared_keys_t() const {
    return _sk;
}

bool shared_keys_t::operator==(shared_keys_t other) const {
    return _sk == other._sk;
}

shared_keys_t::shared_keys_t(const shared_keys_t &other) noexcept
    : _sk(retain(other._sk))
{}

shared_keys_t::shared_keys_t(shared_keys_t &&other) noexcept
    : _sk(other._sk) {
    other._sk = nullptr;
}

shared_keys_t::shared_keys_t(impl_shared_keys_t sk, int)
    : _sk(sk)
{}

void shared_keys_t::write_state(const encoder_t &enc) {
    _sk->write_state(*enc.get_encoder());
}

shared_keys_t shared_keys_t::create(slice_t state) {
    auto sk = create();
    sk.load_state(state);
    return sk;
}

shared_keys_t& shared_keys_t::operator= (const shared_keys_t &other) {
    auto sk = retain(other._sk);
    release(_sk);
    _sk = sk;
    return *this;
}

shared_keys_t& shared_keys_t::operator= (shared_keys_t &&other) noexcept {
    release(_sk);
    _sk = other._sk;
    other._sk = nullptr;
    return *this;
}


doc_t::doc_t(alloc_slice_t data, trust_type trust, shared_keys_t sk, slice_t extern_dest) noexcept
    : _doc(retain(new impl::doc_t(data, trust, sk, extern_dest)))
{}

alloc_slice_t doc_t::dump(nonnull_slice data) {
    try {
        return alloc_slice_t(impl::value_t::dump(data));
    } CATCH_ERROR(nullptr);
    return alloc_slice_t();
}

doc_t::doc_t()
    : _doc(nullptr)
{}

doc_t::doc_t(impl_doc_t d, bool is_retain)
    : _doc(d) {
    if (is_retain) retain(_doc);
}

doc_t::doc_t(const doc_t &other) noexcept
    : _doc(retain(other._doc))
{}

doc_t::doc_t(doc_t &&other) noexcept
    : _doc(other._doc) {
    other._doc = nullptr;
}

doc_t::~doc_t() {
    release(_doc);
}

slice_t doc_t::data() const {
    return _doc ? _doc->data() : slice_t();
}

alloc_slice_t doc_t::alloced_data() const {
    return _doc ? _doc->alloced_data() : alloc_slice_t();
}

shared_keys_t doc_t::shared_keys() const {
    return _doc ? _doc->shared_keys() : nullptr;
}

value_t doc_t::root() const {
    return _doc ? _doc->root() : nullptr;
}

storage::doc_t::operator bool() const {
    return root() != nullptr;
}

array_t doc_t::as_array() const {
    return root().as_array();
}

dict_t doc_t::as_dict() const {
    return root().as_dict();
}

storage::doc_t::operator value_t() const {
    return root();
}

storage::doc_t::operator dict_t() const {
    return as_dict();
}

storage::doc_t::operator impl_dict_t() const {
    return as_dict();
}

value_t doc_t::operator[](int index) const {
    return as_array().get(index);
}

value_t doc_t::operator[](slice_t key) const {
    return as_dict().get(key);
}

value_t doc_t::operator[](const char *key) const {
    return as_dict().get(key);
}

value_t doc_t::operator[](const key_path_t &kp) const {
    return root().operator[](kp);
}

bool doc_t::operator==(const doc_t &d) const {
    return _doc == d._doc;
}

storage::doc_t::operator impl_doc_t() const {
    return _doc;
}

impl_doc_t doc_t::detach() {
    auto d = _doc;
    _doc = nullptr;
    return d;
}

doc_t doc_t::containing(value_t v) {
    return doc_t(v ? retain(impl::doc_t::containing(v).get()) : nullptr, false);
}

bool doc_t::set_associated(void *p, const char *t) {
    return _doc && const_cast<impl::doc_t*>(_doc)->set_associated(p, t);
}

void *doc_t::associated(const char *type) const {
    return _doc ? _doc->get_associated(type) : nullptr;
}

doc_t::doc_t(impl_value_t v)
    : _doc(v ? retain(impl::doc_t::containing(v).get()) : nullptr)
{}

doc_t doc_t::from_json(nonnull_slice json, error_code *out_error) {
    try {
        return doc_t(retain(impl::doc_t::from_json(json)), false);
    } CATCH_ERROR(out_error);
    return doc_t(nullptr, false);
}

doc_t& doc_t::operator=(const doc_t &other) {
    if (other._doc != _doc) {
        release(_doc);
        _doc = retain(other._doc);
    }
    return *this;
}

doc_t& doc_t::operator=(doc_t &&other) noexcept {
    if (other._doc != _doc) {
        release(_doc);
        _doc = other._doc;
        other._doc = nullptr;
    }
    return *this;
}


encoder_t::key_ref_t::key_ref_t(encoder_t &enc, slice_t key)
    : _enc(enc)
    , _key(key)
{}

void encoder_t::key_ref_t::operator= (bool value) {
    _enc.write_key(_key);
    _enc.write_bool(value);
}


encoder_t::encoder_t()
    : _enc(new impl::encoder_impl_t(encode_format::internal, 0, true))
{}

encoder_t::encoder_t(encode_format format, size_t reserve_size, bool unique_strings)
    : _enc(new impl::encoder_impl_t(format, reserve_size, unique_strings))
{}

encoder_t::encoder_t(FILE *file, bool unique_strings)
    : _enc(new impl::encoder_impl_t(file, unique_strings))
{}

encoder_t::encoder_t(shared_keys_t sk)
    : encoder_t() {
    set_shared_keys(sk);
}

encoder_t::encoder_t(impl_encoder_t enc)
    : _enc(enc)
{}

encoder_t::encoder_t(encoder_t &&enc)
    : _enc(enc._enc) {
    enc._enc = nullptr;
}

encoder_t::~encoder_t() {
    delete _enc;
}

void encoder_t::detach() {
    _enc = nullptr;
}

void encoder_t::set_shared_keys(shared_keys_t sk) {
    if (_enc->is_internal())
        _enc->encoder->set_shared_keys(sk);
}

slice_t encoder_t::base() const {
    if (_enc->is_internal())
        return _enc->encoder->base();
    return {};
}

void encoder_t::suppress_trailer() {
    if (_enc->is_internal())
        _enc->encoder->suppress_trailer();
}

storage::encoder_t::operator impl_encoder_t() const {
    return _enc;
}

bool encoder_t::write_raw(slice_t data) {
    ENCODER_TRY(_enc, write_raw(data));
}

size_t encoder_t::next_write_pos() const {
    if (_enc->is_internal())
        return _enc->encoder->next_write_pos();
    return 0;
}

size_t encoder_t::finish_item() {
    if (_enc->is_internal())
        return _enc->encoder->finish_item();
    return 0;
}

encoder_t &encoder_t::operator<<(null_value_t) {
    write_null();
    return *this;
}

encoder_t &encoder_t::operator<<(long long i) {
    write_int(i);
    return *this;
}

encoder_t &encoder_t::operator<<(unsigned long long i) {
    write_uint(i);
    return *this;
}

encoder_t &encoder_t::operator<<(long i) {
    write_int(i);
    return *this;
}

encoder_t &encoder_t::operator<<(unsigned long i) {
    write_uint(i);
    return *this;
}

encoder_t &encoder_t::operator<<(int i) {
    write_int(i);
    return *this;
}

encoder_t &encoder_t::operator<<(unsigned int i) {
    write_uint(i);
    return *this;
}

encoder_t &encoder_t::operator<<(double d) {
    write_double(d);
    return *this;
}

encoder_t &encoder_t::operator<<(float f) {
    write_float(f);
    return *this;
}

encoder_t &encoder_t::operator<<(slice_t s) {
    write_string(s);
    return *this;
}

encoder_t &encoder_t::operator<<(const char *s) {
    write_string(s);
    return *this;
}

encoder_t &encoder_t::operator<<(const std::string &s) {
    write_string(s);
    return *this;
}

encoder_t &encoder_t::operator<<(value_t v) {
    write_value(v);
    return *this;
}

encoder_t::key_ref_t encoder_t::operator[](nonnull_slice key) {
    return key_ref_t(*this, key);
}

impl::encoder_t *encoder_t::get_encoder() const {
    return _enc->encoder.get();
}

impl::json_encoder_t *encoder_t::get_json_encoder() const {
    return _enc->json_encoder.get();
}

void encoder_t::record_exception(const std::exception &x) {
    _enc->record_exception(x);
}

void encoder_t::amend(slice_t base, bool reuse_strings, bool extern_pointers) {
    if (_enc->is_internal() && base.size > 0) {
        _enc->encoder->set_base(base, extern_pointers);
        if (reuse_strings)
            _enc->encoder->reuse_base_strings();
    }
}

bool encoder_t::write_null() {
    ENCODER_TRY(_enc, write_null());
}

bool encoder_t::write_undefined() {
    ENCODER_TRY(_enc, write_undefined());
}

bool encoder_t::write_bool(bool b) {
    ENCODER_TRY(_enc, write_bool(b));
}

bool encoder_t::write_int(int64_t i) {
    ENCODER_TRY(_enc, write_int(i));
}

bool encoder_t::write_uint(uint64_t i) {
    ENCODER_TRY(_enc, write_uint(i));
}

bool encoder_t::write_float(float f) {
    ENCODER_TRY(_enc, write_float(f));
}

bool encoder_t::write_double(double d) {
    ENCODER_TRY(_enc, write_double(d));
}

bool encoder_t::write_string(slice_t s) {
    ENCODER_TRY(_enc, write_string(s));
}

bool encoder_t::write_string(const char *s) {
    return write_string(slice_t(s));
}

bool encoder_t::write_string(std::string s) {
    return write_string(slice_t(s));
}

bool encoder_t::write_date_string(time_stamp ts, bool utc) {
    ENCODER_TRY(_enc, write_date_string(ts, utc));
}

bool encoder_t::write_data(slice_t d) {
    ENCODER_TRY(_enc, write_data(d));
}

bool encoder_t::write_value(value_t v) {
    ENCODER_TRY(_enc, write_value(v));
}

bool encoder_t::convert_json(nonnull_slice json) {
    if (!_enc->has_error()) {
        try {
            if (_enc->is_internal()) {
                auto jc = _enc->json_converter.get();
                if (jc) {
                    jc->reset();
                } else {
                    jc = new impl::json_converter_t(*_enc->encoder);
                    _enc->json_converter.reset(jc);
                }
                if (jc->encode_json(json)) {
                    return true;
                } else {
                    _enc->error = jc->error();
                    _enc->error_message = jc->error_message();
                }
            } else {
                _enc->json_encoder->write_json(json);
                return true;
            }
        } catch (const std::exception &x) {
            _enc->record_exception(x);
        }
    }
    return false;
}

bool encoder_t::begin_array(size_t reserve_count) {
    ENCODER_TRY(_enc, begin_array(reserve_count));
}

bool encoder_t::end_array() {
    ENCODER_TRY(_enc, end_array());
}

bool encoder_t::begin_dict(size_t reserve_count) {
    ENCODER_TRY(_enc, begin_dict(reserve_count));
}

bool encoder_t::write_key(nonnull_slice key) {
    ENCODER_TRY(_enc, write_key(key));
}

bool encoder_t::write_key(value_t key) {
    ENCODER_TRY(_enc, write_key(key));
}

bool encoder_t::end_dict() {
    ENCODER_TRY(_enc, end_dict());
}

size_t encoder_t::bytes_written_size() const {
    return ENCODER_DO(_enc, bytes_written_size());
}

doc_t encoder_t::finish_doc(error_code* err) {
    if (_enc->encoder) {
        if (!_enc->has_error()) {
            try {
                return retain(_enc->encoder->finish_doc());
            } catch (const std::exception &x) {
                _enc->record_exception(x);
            }
        }
    } else {
        _enc->error = error_code::unsupported;
    }
    if (err) *err = _enc->error;
    _enc->reset();
    return nullptr;
}

alloc_slice_t encoder_t::finish(error_code* err) {
    if (!_enc->has_error()) {
        try {
            return ENCODER_DO(_enc, finish());
        } catch (const std::exception &x) {
            _enc->record_exception(x);
        }
    }
    if (err) *err = _enc->error;
    _enc->reset();
    return alloc_slice_t();
}

void encoder_t::reset() {
    return _enc->reset();
}

error_code encoder_t::error() const {
    return _enc->error;
}

const char* encoder_t::error_message() const {
    return _enc->has_error() ? _enc->error_message.c_str() : nullptr;
}


json_encoder_t::json_encoder_t()
    : encoder_t(encode_format::json)
{}

bool json_encoder_t::write_raw(slice_t raw) {
    ENCODER_TRY(_enc, write_raw(raw));
}


shared_encoder_t::shared_encoder_t(impl_encoder_t enc)
    : encoder_t(enc)
{}

shared_encoder_t::~shared_encoder_t() {
    detach();
}


alloc_slice_t json_delta_t::create(value_t old, value_t nuu) {
    try {
        return impl::json_delta_t::create(old, nuu);
    } catch (const std::exception&) {
        return alloc_slice_t();
    }
}

bool json_delta_t::create(value_t old, value_t nuu, encoder_t &encoder) {
    try {
        auto enc = encoder.get_json_encoder();
        precondition(enc);
        impl::json_delta_t::create(old, nuu, *enc);
        return true;
    } catch (const std::exception &x) {
        encoder.record_exception(x);
        return false;
    }
}

alloc_slice_t json_delta_t::apply(value_t old, slice_t json_delta, error_code *error) {
    try {
        return impl::json_delta_t::apply(old, json_delta);
    } CATCH_ERROR(error);
    return alloc_slice_t();
}

bool json_delta_t::apply(value_t old, slice_t json_delta, encoder_t &encoder) {
    try {
        auto enc = encoder.get_encoder();
        if (!enc)
            exception_t::_throw(error_code::encode_error, "encode_applying_json_delta cannot encode JSON");
        impl::json_delta_t::apply(old, json_delta, *enc);
        return true;
    } catch (const std::exception &x) {
        encoder.record_exception(x);
        return false;
    }
}


alloced_dict_t::alloced_dict_t(alloc_slice_t s)
    : dict_t(value_t::from_data(s).as_dict())
    , alloc_slice_t(std::move(s))
{}

alloced_dict_t::alloced_dict_t(slice_t s)
    : alloced_dict_t(alloc_slice_t(s))
{}

const alloc_slice_t &alloced_dict_t::data() const {
    return *this;
}

storage::alloced_dict_t::operator bool() const {
    return dict_t::operator bool();
}

value_t alloced_dict_t::operator[](slice_t key) const {
    return dict_t::get(key);
}

value_t alloced_dict_t::operator[](const char *key) const {
    return dict_t::get(key);
}

}
