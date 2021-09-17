#include "json_converter.hpp"
#include "num_conversion.hpp"
#include "boost/json.hpp"
#include <map>

namespace storage { namespace impl {

json_converter_t::json_converter_t(encoder_t &enc) noexcept
    : _encoder(enc)
    , _json_error(no_error)
    , _error_pos(0)
{
}

json_converter_t::~json_converter_t() {
}

void json_converter_t::reset() {
    _json_error = no_error;
    _error_pos = 0;
}

const char* json_converter_t::error_message() noexcept {
    if (!_error_message.empty())
        return _error_message.c_str();
    else if (_json_error == error_exception_thrown)
        return "Unexpected C++ exception";
    else if (_json_error == error_truncated_json)
        return "Truncated JSON";
    return "";
}

size_t json_converter_t::error_pos() noexcept {
    return _error_pos;
}

bool json_converter_t::encode_json(slice_t json) {
    _input = json;
    _error_message.clear();
    _error = error_code::no_error;
    _json_error = no_error;
    _error_pos = 0;

    using namespace boost;
    try {
        json::error_code error;
        json::value value = json::parse(std::string(json), error);
        if (error) {
            _json_error = error_truncated_json; //todo error.value()
            _error = error_code::json_error;
            _error_pos = json.size;
        } else {
            write_value(value);
        }
    } catch (std::bad_alloc const& e) {
        _json_error = error_exception_thrown;
        _error = error_code::json_error;
        _error_message = e.what();
    }
    return _json_error == no_error;
}

int json_converter_t::json_error() noexcept {
    return _json_error;
}

error_code json_converter_t::error() noexcept {
    return _error;
}

alloc_slice_t json_converter_t::convert_json(slice_t json, shared_keys_t *sk) {
    encoder_t enc;
    enc.set_shared_keys(sk);
    json_converter_t cvt(enc);
    _throw_if(!cvt.encode_json(slice_t(json)), error_code::json_error, cvt.error_message());
    return enc.finish();
}

void json_converter_t::write_value(const boost::json::value &value)
{
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

} }
