#include "msgpack_encoder.hpp"


class json_converter_t {
    enum {
        no_error = 0,
        error_truncated_json = 1000,
        error_exception_thrown
    };

public:
    json_converter_t(encoder_t &enc) noexcept;

    bool encode_json(slice_t json);
    std::string error_message() noexcept;

private:
    void write_value(const boost::json::value &value);

    encoder_t &_encoder;
    int _error {no_error};
    std::string _error_message;
};



alloc_slice_t json_coder::from_json(encoder_t &enc, slice_t json) {
    json_converter_t converter(enc);
    _throw_if(!converter.encode_json(slice_t(json)), error_code::json_error, converter.error_message().c_str());
    return enc.finish();
}

alloc_slice_t json_coder::from_json(slice_t json, shared_keys_t *sk) {
    encoder_t enc;
    enc.set_shared_keys(sk);
    return from_json(enc, json);
}




json_encoder_t::json_encoder_t(size_t reserve_output_size)
    : _out(reserve_output_size)
{}

void json_encoder_t::set_canonical(bool canonical) {
    _canonical = canonical;
}

bool json_encoder_t::empty() const {
    return _out.length() == 0;
}

size_t json_encoder_t::bytes_written_size() const {
    return _out.length();
}

alloc_slice_t json_encoder_t::finish() {
    return _out.finish();
}

void json_encoder_t::reset() {
    _out.reset();
    _first = true;
}

void json_encoder_t::write_null() {
    comma();
    _out << slice_t("null");
}

void json_encoder_t::write_bool(bool b) {
    comma();
    _out.write(b ? "true"_sl : "false"_sl);
}

void json_encoder_t::write_int(int64_t i) {
    _write_int("%lld", i);
}

void json_encoder_t::write_uint(uint64_t i) {
    _write_int("%llu", i);
}

void json_encoder_t::write_float(float f) {
    _write_float(f);
}

void json_encoder_t::write_double(double d) {
    _write_float(d);
}

void json_encoder_t::write_string(const std::string &s) {
    write_string(slice_t(s));
}

void json_encoder_t::write_string(slice_t str) {
    comma();
    _out << '"';
    auto start = (const uint8_t*)str.buf;
    auto end = (const uint8_t*)str.end();
    for (auto p = start; p < end; p++) {
        uint8_t ch = *p;
        if (ch == '"' || ch == '\\' || ch < 32 || ch == 127) {
            _out.write({start, p});
            start = p + 1;
            switch (ch) {
                case '"':
                case '\\':
                    _out << '\\';
                    --start;
                    break;
                case '\r':
                    _out.write("\\r"_sl);
                    break;
                case '\n':
                    _out.write("\\n"_sl);
                    break;
                case '\t':
                    _out.write("\\t"_sl);
                    break;
                default: {
                    char buf[7];
                    _out.write(buf, sprintf(buf, "\\u%04x", (unsigned)ch));
                    break;
                }
            }
        }
    }
    if (end > start)
        _out.write({start, end});
    _out << '"';
}

void json_encoder_t::write_date_string(int64_t timestamp, bool utc) {
    char str[formatted_iso8601_date_max_size];
    write_string(format_iso8601_date(str, timestamp, utc));
}

void json_encoder_t::write_data(slice_t d) {
    comma();
    _out << '"';
    _out.write_base64(d);
    _out << '"';
}

void json_encoder_t::write_key(slice_t s) {
    assert_precondition(s);
    write_string(s);
    _out << ':';
    _first = true;
}

void json_encoder_t::write_key(const std::string &s) {
    write_key(slice_t(s));
}

void json_encoder_t::write_key(const value_t *v) {
    write_key(v->as_string());
}

json_encoder_t &json_encoder_t::operator<<(long long i) {
    write_int(i);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(unsigned long long i) {
    write_uint(i);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(long i) {
    write_int(i);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(unsigned long i) {
    write_uint(i);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(int i) {
    write_int(i);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(unsigned int i) {
    write_uint(i);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(double d) {
    write_double(d);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(float f) {
    write_float(f);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(const std::string &str) {
    write_string(str);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(slice_t s) {
    write_string(s);
    return *this;
}

json_encoder_t &json_encoder_t::operator<<(const value_t *v) {
    write_value(v);
    return *this;
}

void json_encoder_t::begin_array(size_t) {
    begin_array();
}

void json_encoder_t::begin_dict(size_t) {
    begin_dict();
}

void json_encoder_t::write_undefined() {
    exception_t::_throw(error_code::json_error, "Cannot write `undefined` to JSON encoder");
}

void json_encoder_t::write_dict(const dict_t *dict) {
    begin_dict();
    if (_canonical) {
        struct kv {
            slice_t key;
            const value_t *value;
            bool operator< (const kv &other) const  { return key < other.key; }
        };
        small_vector<kv, 4> items;
        items.reserve(dict->count());
        for (auto iter = dict->begin(); iter; ++iter)
            items.push_back({iter.key_string(), iter.value()});
        std::sort(items.begin(), items.end());
        for (auto &item : items) {
            write_key(item.key);
            write_value(item.value);
        }
    } else {
        for (auto iter = dict->begin(); iter; ++iter) {
            slice_t key_str = iter.key_string();
            if (key_str) {
                write_key(key_str);
            } else {
                comma();
                _first = true;
                write_value(iter.key());
                _out << ':';
                _first = true;
            }
            write_value(iter.value());
        }
    }
    end_dict();
}

template <class T>
void json_encoder_t::_write_int(const char *fmt, T t) {
    comma();
    char str[32];
    _out.write(str, sprintf(str, fmt, t));
}

template <class T>
void json_encoder_t::_write_float(T t) {
    comma();
    char str[32];
    _out.write(str, document::write_float(t, str, sizeof(str)));
}

void json_encoder_t::comma() {
    if (_first)
        _first = false;
    else
        _out << ',';
}

void json_encoder_t::write_value(const value_t *v) {
    switch (v->type()) {
        case value_type::null:
        case value_type::undefined:
            if (v->is_undefined()) {
                comma();
                _out << slice_t("undefined");
            } else {
                write_null();
            }
            break;
        case value_type::boolean:
            write_bool(v->as_bool());
            break;
        case value_type::number:
            if (v->is_int()) {
                auto i = v->as_int();
                if (v->is_unsigned())
                    write_uint(i);
                else
                    write_int(i);
            } else if (v->is_double()) {
                write_double(v->as_double());
            } else {
                write_float(v->as_float());
            }
            break;
        case value_type::string:
            write_string(v->as_string());
            break;
        case value_type::data:
            write_data(v->as_data());
            break;
        case value_type::array:
            begin_array();
            for (auto iter = v->as_array()->begin(); iter; ++iter)
                write_value(iter.value());
            end_array();
            break;
        case value_type::dict:
            write_dict(v->as_dict());
            break;
        default:
            exception_t::_throw(error_code::unknown_value, "illegal typecode in value_t; corrupt data?");
    }
}

void json_encoder_t::write_json(slice_t json) {
    comma();
    _out << json;
}

void json_encoder_t::write_raw(slice_t raw) {
    _out << raw;
}

void json_encoder_t::begin_array() {
    comma();
    _out << '[';
    _first = true;
}

void json_encoder_t::end_array() {
    _out << ']';
    _first = false;
}

void json_encoder_t::begin_dict() {
    comma();
    _out << '{';
    _first = true;
}

void json_encoder_t::end_dict() {
    _out << '}';
    _first = false;
}


json_converter_t::json_converter_t(encoder_t &enc) noexcept
    : _encoder(enc)
    , _error(no_error)
{}

bool json_converter_t::encode_json(slice_t json) {
    _error_message.clear();
    _error = no_error;

    using namespace boost;
    try {
        json::error_code error;
        json::value value = json::parse(std::string(json), error);
        if (error) {
            _error = error.value();
            _error_message = error.message();
        } else {
            write_value(value);
        }
    } catch (std::bad_alloc const& e) {
        _error = error_exception_thrown;
        _error_message = e.what();
    }
    return _error == no_error;
}

std::string json_converter_t::error_message() noexcept {
    if (!_error_message.empty())
        return _error_message;
    else if (_error == error_truncated_json)
        return std::string("Truncated JSON");
    else if (_error == error_exception_thrown)
        return std::string("Unexpected C++ exception");
    return std::string();
}

void json_converter_t::write_value(const boost::json::value &value) {
    if (value.is_null()) {
        _encoder.write_null();
    } else if (value.is_bool()) {
        _encoder.write_bool(value.as_bool());
    } else if (value.is_int64()) {
        _encoder.write_int(value.as_int64());
    } else if (value.is_uint64()) {
        _encoder.write_uint(value.as_uint64());
    } else if (value.is_double()) {
        _encoder.write_double(value.as_double());
    } else if (value.is_string()) {
        _encoder.write_string(value.as_string().c_str());
    } else if (value.is_array()) {
        _encoder.begin_array();
        for (auto elem : value.as_array()) {
            write_value(elem);
        }
        _encoder.end_array();
    } else if (value.is_object()) {
        _encoder.begin_dict();
        for (auto elem : value.as_object()) {
            _encoder.write_key(elem.key_c_str());
            write_value(elem.value());
        }
        _encoder.end_dict();
    }
}