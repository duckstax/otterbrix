#include "json_converter.hpp"
#include "num_conversion.hpp"
#include "jsonsl.hpp"
#include <map>

namespace storage { namespace impl {

static int error_callback(struct jsonsl_st * jsn, jsonsl_error_t err, struct jsonsl_state_st *state, char *errat) noexcept;
static void write_push_callback(struct jsonsl_st * jsn, jsonsl_action_t action, struct jsonsl_state_st *state, const char *buf) noexcept;
static void write_pop_callback(struct jsonsl_st * jsn, jsonsl_action_t action, struct jsonsl_state_st *state, const char *buf) noexcept;

json_converter_t::json_converter_t(encoder_t &enc) noexcept
    : _encoder(enc)
    , _jsn(jsonsl_new(50))
    , _json_error(JSONSL_ERROR_SUCCESS)
    , _error_pos(0)
{
    _jsn->data = this;
}

json_converter_t::~json_converter_t() {
    jsonsl_destroy(_jsn);
}

void json_converter_t::reset() {
    jsonsl_reset(_jsn);
    _json_error = JSONSL_ERROR_SUCCESS;
    _error_pos = 0;
}

const char* json_converter_t::error_message() noexcept {
    if (!_error_message.empty())
        return _error_message.c_str();
    else if (_json_error == error_exception_thrown)
        return "Unexpected C++ exception";
    else if (_json_error == error_truncated_json)
        return "Truncated JSON";
    else {
        _error_message = std::string("JSON parse error: ") + jsonsl_strerror((jsonsl_error_t)_json_error);
        return _error_message.c_str();
    }
}

size_t json_converter_t::error_pos() noexcept {
    return _error_pos;
}

bool json_converter_t::encode_json(slice_t json) {
    _input = json;
    _error_message.clear();
    _error = error_code::no_error;
    _json_error = JSONSL_ERROR_SUCCESS;
    _error_pos = 0;

    _jsn->data = this;
    _jsn->action_callback_PUSH = write_push_callback;
    _jsn->action_callback_POP  = write_pop_callback;
    _jsn->error_callback = error_callback;
    jsonsl_enable_all_callbacks(_jsn);

    jsonsl_feed(_jsn, (char*)json.buf, json.size);
    if (_jsn->level > 0 && !_json_error) {
        _json_error = error_truncated_json;
        _error = error_code::json_error;
        _error_pos = json.size;
    }
    jsonsl_reset(_jsn);
    return (_json_error == JSONSL_ERROR_SUCCESS);
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

inline void json_converter_t::push(struct jsonsl_state_st *state) {
    switch (state->type) {
    case JSONSL_T_LIST:
        _encoder.begin_array();
        break;
    case JSONSL_T_OBJECT:
        _encoder.begin_dict();
        break;
    }
}

void json_converter_t::write_double(struct jsonsl_state_st *state) {
    char *start = (char*)&_input[state->pos_begin];
    _encoder.write_double(parse_double(start));
}

inline void json_converter_t::pop(struct jsonsl_state_st *state) {
    switch (state->type) {
    case JSONSL_T_SPECIAL: {
        unsigned f = state->special_flags;
        if (f & JSONSL_SPECIALf_FLOAT || f & JSONSL_SPECIALf_EXPONENT) {
            char *start = (char*)&_input[state->pos_begin];
            _encoder.write_double(parse_double(start));
        } else if (f & JSONSL_SPECIALf_UNSIGNED) {
            if (_usually_true(state->pos_cur - state->pos_begin < 19)) {
                _encoder.write_uint(state->nelem);
            } else {
                char *start = (char*)&_input[state->pos_begin];
                uint64_t n;
                if (parse_unsigned_integer(start, n, true))
                    _encoder.write_uint(n);
                else
                    write_double(state);
            }
        } else if (f & JSONSL_SPECIALf_SIGNED) {
            if (_usually_true(state->pos_cur - state->pos_begin < 20)) {
                _encoder.write_int(-(int64_t)state->nelem);
            } else {
                char *start = (char*)&_input[state->pos_begin];
                int64_t n;
                if (parse_integer(start, n, true))
                    _encoder.write_int(n);
                else
                    write_double(state);
            }
        } else if (f & JSONSL_SPECIALf_TRUE) {
            _encoder.write_bool(true);
        } else if (f & JSONSL_SPECIALf_FALSE) {
            _encoder.write_bool(false);
        } else if (f & JSONSL_SPECIALf_NULL) {
            _encoder.write_null();
        }
        break;
    }
    case JSONSL_T_STRING:
    case JSONSL_T_HKEY: {
        slice_t str(&_input[state->pos_begin + 1], state->pos_cur - state->pos_begin - 1);
        char *buf = nullptr;
        bool malloced_buf = false;
        if (state->nescapes > 0) {
            malloced_buf = str.size > 100;
            buf = (char*)(malloced_buf ? malloc(str.size) : alloca(str.size));
            jsonsl_error_t err = JSONSL_ERROR_SUCCESS;
            const char *errat;
            auto size = jsonsl_util_unescape_ex((const char*)str.buf, buf, str.size, nullptr, nullptr, &err, &errat);
            if (err) {
                error_callback(_jsn, err, state, (char*)errat);
                if (malloced_buf)
                    free(buf);
                return;
            }
            str = slice_t(buf, size);
        }
        if (state->type == JSONSL_T_STRING)
            _encoder.write_string(str);
        else
            _encoder.write_key(str);
        if (malloced_buf)
            free(buf);
        break;
    }
    case JSONSL_T_LIST:
        _encoder.end_array();
        break;
    case JSONSL_T_OBJECT:
        _encoder.end_dict();
        break;
    }
}

int json_converter_t::got_error(int err, size_t pos) noexcept {
    _json_error = err;
    _error_pos = pos;
    _error = error_code::json_error;
    jsonsl_stop(_jsn);
    return 0;
}

int json_converter_t::got_error(int err, const char *errat) noexcept {
    return got_error(err, errat ? (errat - (char*)_input.buf) : 0);
}

void json_converter_t::got_exception(error_code code, const char *what, size_t pos) noexcept {
    got_error(error_exception_thrown, pos);
    _error = code;
    _error_message = what;
}


static inline json_converter_t* converter(jsonsl_t jsn) noexcept {
    return (json_converter_t*)jsn->data;
}

static void write_push_callback(jsonsl_t jsn, jsonsl_action_t, struct jsonsl_state_st *state, const char *) noexcept {
    try {
        converter(jsn)->push(state);
    } catch (const exception_t &x) {
        converter(jsn)->got_exception(x.code, x.what(), state->pos_begin);
    } catch (...) {
        converter(jsn)->got_exception(error_code::internal_error, nullptr, state->pos_begin);
    }
}

static void write_pop_callback(jsonsl_t jsn, jsonsl_action_t, struct jsonsl_state_st *state, const char *) noexcept {
    try {
        converter(jsn)->pop(state);
    } catch (const exception_t &x) {
        converter(jsn)->got_exception(x.code, x.what(), state->pos_begin);
    } catch (...) {
        converter(jsn)->got_exception(error_code::internal_error, nullptr, state->pos_begin);
    }
}

static int error_callback(jsonsl_t jsn, jsonsl_error_t err, struct jsonsl_state_st *, char *errat) noexcept {
    return converter(jsn)->got_error(err, errat);
}

} }
