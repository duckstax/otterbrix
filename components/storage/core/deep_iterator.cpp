#include "deep_iterator.hpp"
#include "shared_keys.hpp"
#include <sstream>

namespace storage { namespace impl {

deep_iterator_t::deep_iterator_t(const value_t *root)
    : _value(root)
    , _skip_children(false)
{}

void deep_iterator_t::next() {
    if (!_value)
        return;

    if (_skip_children)
        _skip_children = false;
    else if (_path.empty())
        iterate_container(_value);
    else
        queue_children();

    if (!_path.empty())
        _path.pop_back();

    do {
        if (_array_iterator) {
            _value = (*_array_iterator).value();
            if (_value) {
                _path.push_back({null_slice, _array_index++});
                ++(*_array_iterator);
            } else {
                _array_iterator.reset();
            }
        } else if (_dict_iterator) {
            _value = (*_dict_iterator).value();
            if (_value) {
                _path.push_back({(*_dict_iterator).key_string(), 0});
                ++(*_dict_iterator);
            } else {
                if (!_sk)
                    _sk = _dict_iterator->shared_keys();
                _dict_iterator.reset();
            }
        } else {
            _value = nullptr;
            if (_stack.empty())
                return;
            while (_stack.front().second == nullptr) {
                if (_path.empty())
                    return;
                _path.pop_back();
                _stack.pop_front();
            }

            auto container = _stack.front().second;
            _path.push_back(_stack.front().first);
            _stack.pop_front();
            iterate_container(container);
        }
    } while (!_value);
}

bool deep_iterator_t::iterate_container(const value_t *container) {
    _container = container;
    _stack.push_front({{null_slice, 0}, nullptr});
    auto type = container->type();
    if (type == value_type::array) {
        _array_iterator.reset( new array_t::iterator(container->as_array()) );
        _array_index = 0;
        return true;
    } else if (type == value_type::dict) {
        _dict_iterator.reset( new dict_t::iterator(container->as_dict(), _sk) );
        return true;
    } else {
        return false;
    }
}

void deep_iterator_t::queue_children() {
    auto type = _value->type();
    if (type == value_type::dict || type == value_type::array)
        _stack.push_front({_path.back(), _value});
}

std::string deep_iterator_t::path_string() const {
    std::stringstream s;
    for (auto &component : _path) {
        if (component.key) {
            bool quote = false;
            for (auto c : component.key) {
                if (!isalnum(c) && c != '_') {
                    quote = true;
                    break;
                }
            }
            s << (quote ? "[\"" : ".");
            s.write((char*)component.key.buf, component.key.size);
            if (quote)
                s << "\"]";
        } else {
            s << '[' << component.index << ']';
        }
    }
    return s.str();
}

std::string deep_iterator_t::json_pointer() const {
    if (_path.empty())
        return "/";
    std::stringstream s;
    for (auto &component : _path) {
        s << '/';
        if (component.key) {
            if (component.key.find_any_byte_of("/~"_sl)) {
                auto end = (const char*)component.key.end();
                for (auto c = (const char*)component.key.buf; c != end; ++c) {
                    if (*c == '/')
                        s << "~1";
                    else if (*c == '~')
                        s << "~0";
                    else
                        s << *c;
                }
            } else {
                s.write((char*)component.key.buf, component.key.size);
            }
        } else {
            s << component.index;
        }
    }
    return s.str();
}

} }
