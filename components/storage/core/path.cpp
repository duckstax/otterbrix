#include "path.hpp"
#include "shared_keys.hpp"
#include "exception.hpp"
#include "platform_compat.hpp"
#include "slice_stream.hpp"
#include <iostream>
#include <sstream>

namespace storage { namespace impl {

path_t::element_t::element_t(slice_t property)
    : _key_buf(property)
    , _key(new dict_t::key_t(_key_buf))
{}

path_t::element_t::element_t(int32_t array_index)
    : _index(array_index)
{}

path_t::element_t::element_t(const element_t &other)
    : _key_buf(other._key_buf)
    , _index(other._index)
{
    if (other._key)
        _key.reset(new dict_t::key_t(_key_buf));
}

bool path_t::element_t::operator== (const element_t &e) const {
    return _key ? (_key == e._key) : (_index == e._index);
}

path_t::element_t &path_t::element_t::operator=(path_t::element_t &&other) {
    this->~element_t();
    return *new(this) element_t(std::move(other));
}

bool path_t::element_t::is_key() const {
    return _key != nullptr;
}

dict_t::key_t &path_t::element_t::key() const {
    return *_key;
}

slice_t path_t::element_t::key_str() const {
    return _key ? _key->string() : slice_t();
}

int32_t path_t::element_t::index() const {
    return _index;
}

const value_t* path_t::element_t::eval(const value_t *item) const noexcept {
    if (_key) {
        auto d = item->as_dict();
        if (_usually_false(!d))
            return nullptr;
        return d->get(*_key);
    } else {
        return get_from_array(item, _index);
    }
}

const value_t* path_t::element_t::eval(char token, slice_t comp, int32_t index, const value_t *item) noexcept {
    if (token == '.') {
        auto d = item->as_dict();
        if (_usually_false(!d))
            return nullptr;
        return d->get(comp);
    } else {
        return get_from_array(item, index);
    }
}

const value_t* path_t::element_t::get_from_array(const value_t* item, int32_t index) noexcept {
    auto a = item->as_array();
    if (_usually_false(!a))
        return nullptr;
    if (index < 0) {
        uint32_t count = a->count();
        if (_usually_false((uint32_t)-index > count))
            return nullptr;
        index += count;
    }
    return a->get((uint32_t)index);
}


void path_t::add_components(slice_t components) {
    for_each_component(components, _path.empty(), [&](char token, slice_t component, int32_t index) {
        if (token == '.')
            _path.emplace_back(component);
        else
            _path.emplace_back(index);
        return true;
    });
}

path_t::path_t(slice_t specifier) {
    add_components(specifier);
}

void path_t::add_property(slice_t key) {
    _throw_if(key.size == 0, error_code::path_syntax_error, "Illegal empty property name");
    _path.emplace_back(key);
}

void path_t::add_index(int index) {
    _path.emplace_back(index);
}

path_t& path_t::operator += (const path_t &other) {
    _path.reserve(_path.size() + other.size());
    for (auto &elem : other._path)
        _path.push_back(elem);
    return *this;
}

void path_t::drop(size_t num_to_drop_from_start) {
    _path.erase(_path.begin(), _path.begin() + num_to_drop_from_start);
}

const small_vector<path_t::element_t, 4> &path_t::path() const {
    return _path;
}

small_vector<path_t::element_t, 4> &path_t::path() {
    return _path;
}

bool path_t::empty() const {
    return _path.empty();
}

size_t path_t::size() const {
    return _path.size();
}

const path_t::element_t &path_t::operator[](size_t i) const {
    return _path[i];
}

path_t::element_t &path_t::operator[](size_t i) {
    return _path[i];
}

bool path_t::operator== (const path_t &other) const {
    return _path == other._path;
}

bool path_t::operator!=(const path_t &other) const {
    return !(*this == other);
}

path_t::operator std::string() {
    std::stringstream out;
    write_to(out);
    return out.str();
}

void path_t::write_to(std::ostream &out) const {
    bool first = true;
    for (auto &element : _path) {
        if (element.is_key())
            write_property(out, element.key().string(), first);
        else
            write_index(out, element.index());
        first = false;
    }
}

void path_t::write_property(std::ostream &out, slice_t key, bool first) {
    if (first) {
        if (key.has_prefix('$'))
            out << '\\';
    } else {
        out << '.';
    }
    const uint8_t *quote;
    while (nullptr != (quote = key.find_any_byte_of(".[\\"_sl))) {
        out.write((const char *)key.buf, quote - (const uint8_t*)key.buf);
        out << '\\' << *quote;
        key.set_start(quote + 1);
    }
    out.write((const char *)key.buf, key.size);
}

void path_t::write_index(std::ostream &out, int array_index) {
    out << '[' << array_index << ']';
}

const value_t* path_t::eval(const value_t *root) const noexcept {
    const value_t *item = root;
    if (_usually_false(!item))
        return nullptr;
    for (auto &e : _path) {
        item = e.eval(item);
        if (!item)
            break;
    }
    return item;
}

const value_t* path_t::eval(slice_t specifier, const value_t *root) {
    const value_t *item = root;
    if (_usually_false(!item))
        return nullptr;
    for_each_component(specifier, true, [&](char token, slice_t component, int32_t index) {
        item = element_t::eval(token, component, index, item);
        return (item != nullptr);
    });
    return item;
}

const value_t* path_t::eval_json_pointer(slice_t specifier, const value_t *root) {
    slice_istream in(specifier);
    auto current = root;
    _throw_if(in.read_byte() != '/', error_code::path_syntax_error, "json_pointer does not start with '/'");
    while (!in.eof()) {
        if (!current)
            return nullptr;

        auto slash = in.find_byte_or_end('/');
        slice_istream param(in.buf, slash);

        switch(current->type()) {
        case value_type::array: {
            auto i = param.read_decimal();
            if (_usually_false(param.size > 0 || i > INT32_MAX))
                exception_t::_throw(error_code::path_syntax_error, "Invalid array index in json_pointer");
            current = ((const array_t*)current)->get((uint32_t)i);
            break;
        }
        case value_type::dict: {
            std::string key = param.as_string();
            current = ((const dict_t*)current)->get(key);
            break;
        }
        default:
            current = nullptr;
            break;
        }

        if (slash == in.end())
            break;
        in.set_start(slash+1);
    }
    return current;
}

void path_t::for_each_component(slice_t specifier, bool at_start, each_component_callback callback) {
    slice_istream in(specifier);
    _throw_if(in.size == 0, error_code::path_syntax_error, "Empty path");
    _throw_if(in[in.size-1] == '\\', error_code::path_syntax_error, "'\\' at end of string");

    uint8_t token = in.peek_byte();
    if (token == '$') {
        _throw_if(!at_start, error_code::path_syntax_error, "Illegal $ in path");
        in.skip(1);
        if (in.size == 0)
            return;
        token = in.read_byte();
        _throw_if(token != '.' && token != '[', error_code::path_syntax_error, "Invalid path delimiter after $");
    } else if (token == '[' || token == '.') {
        in.skip(1);
    } else if (token == '\\') {
        if (in[1] == '$')
            in.skip(1);
        token = '.';
    } else {
        token = '.';
    }

    if (in.size == 0 && token == '.')
        return;

    while (true) {
        const uint8_t* next;
        slice_t param;
        alloc_slice_t unescaped;
        int32_t index = 0;

        if (token == '.') {
            next = in.find_any_byte_of(".[\\"_sl);
            if (next == nullptr) {
                param = in;
                next = (const uint8_t*)in.end();
            } else if (*next != '\\') {
                param = slice_t(in.buf, next);
            } else {
                unescaped.reset(in.size);
                auto dst = (uint8_t*)unescaped.buf;
                for (next = (const uint8_t*)in.buf; next < in.end(); ++next) {
                    uint8_t c = *next;
                    if (c == '\\') {
                        c = *++next;
                    } else if(c == '.' || c == '[') {
                        break;
                    }

                    *dst++ = c;
                }
                param = slice_t(unescaped.buf, dst);
            }

        } else if (token == '[') {
            next = in.find_byte_or_end(']');
            if (!next)
                exception_t::_throw(error_code::path_syntax_error, "Missing ']'");
            param = slice_t(in.buf, next++);
            slice_istream n = param;
            int64_t i = n.read_signed_decimal();
            _throw_if(param.size == 0 || n.size > 0 || i > INT32_MAX || i < INT32_MIN,
                      error_code::path_syntax_error, "Invalid array index");
            index = (int32_t)i;
        } else {
            exception_t::_throw(error_code::path_syntax_error, "Invalid path component");
        }

        if (param.size > 0) {
            if (_usually_false(!callback(token, param, index)))
                return;
        }

        if (next >= in.end())
            break;

        token = *next;
        in.set_start(next+1);
    }
}

} }
