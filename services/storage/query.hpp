#pragma once
#include <vector>
#include <functional>
#include <string>
#include <algorithm>
#include <regex>

namespace services::storage {

template <class VALUE>
class condition_t {
    std::string _cond;
    std::string _key;
    VALUE _value;
    std::vector<condition_t> _subcond;

public:
    condition_t(std::string &&cond, const std::string &key, VALUE value)
        : _cond(std::move(cond))
        , _key(key)
        , _value(value)
    {}

    condition_t operator &(condition_t &&other) noexcept {
        if (this->_cond == "and") {
            this->_subcond << std::move(other);
            return *this;
        } else if (other._cond == "and") {
            other._subcond << std::move(*this);
            return other;
        }
        return condition_t{"and", {std::move(*this), std::move(other)}};
    }

    condition_t operator |(condition_t &&other) noexcept {
        if (this->_cond == "or") {
            this->_subcond << std::move(other);
            return *this;
        } else if (other._cond == "or") {
            other._subcond << std::move(*this);
            return other;
        }
        return condition_t{"or", {std::move(*this), std::move(other)}};
    }

    condition_t operator !() noexcept {
        if (this->_cond == "not" && !this->_subcond.empty()) {
            return this->_subcond[0];
        }
        return condition_t{"not", {std::move(*this)}};
    }

#ifdef DEV_MODE
    template <class T> static std::string to_str(T value) {
        return std::to_string(value);
    }

    static std::string to_str(std::string value) {
        return "\"" + value + "\"";
    }

    static std::string to_str(const char *value) {
        return std::string(value);
    }

    std::string debug() const {
        return _key + _cond + to_str(_value);
    }
#endif

private:
    condition_t(std::string &&cond, std::vector<condition_t> &&subcond)
        : _cond(std::move(cond))
        , _value(VALUE())
        , _subcond(std::move(subcond))
    {}
};


template <class VALUE>
class query_t {
    std::function<bool(VALUE)> _check;

public:
    explicit query_t(std::function<bool(VALUE)> check)
        : _check(check)
    {}

    bool check(const VALUE &v) const {
        return _check(v);
    }

    query_t operator &(const query_t &other) noexcept {
        return query_t{[&](VALUE v){ return this->check(v) && other.check(v); }};
    }

    query_t operator |(const query_t &other) noexcept {
        return query_t{[&](VALUE v){ return this->check(v) || other.check(v); }};
    }

    query_t operator !() noexcept {
        return query_t{[&](VALUE v){ return !this->check(v); }};
    }

    query_t operator ==(const VALUE &value) noexcept { //?
        return query_t([&](VALUE v){ return v == value; });
    }

    query_t operator !=(const VALUE &value) noexcept { //?
        return query_t([&](VALUE v){ return v != value; });
    }

    query_t operator <(const VALUE &value) noexcept { //?
        return query_t([&](VALUE v){ return v < value; });
    }

    query_t operator >(const VALUE &value) noexcept { //?
        return query_t([&](VALUE v){ return v > value; });
    }

    query_t operator <=(const VALUE &value) noexcept { //?
        return query_t([&](VALUE v){ return v <= value; });
    }

    query_t operator >=(const VALUE &value) noexcept { //?
        return query_t([&](VALUE v){ return v >= value; });
    }

    static query_t between(const VALUE &v1, const VALUE &v2) {
        return query_t([&](VALUE v){ return v1 <= v && v <= v2; });
    }

    template <class CONT>
    static query_t any(const CONT &array) {
        return query_t([&](VALUE v){
            return std::find(array.begin(), array.end(), v) != array.end();
        });
    }

    template <class CONT>
    static query_t all(const CONT &array) {
        return query_t([&](VALUE v){
            return std::find_if(array.begin(), array.end(), [&](VALUE v2){ return v2 != v; }) == array.end();
        });
    }

    static query_t matches(std::regex &&regex) {
        return query_t([&](VALUE v){
            return std::regex_search(v, regex);
        });
    }

    static query_t matches(const std::string &regex) {
        return query_t::matches(std::regex(regex));
    }
};

}
