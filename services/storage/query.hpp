#pragma once
#include <functional>
#include <string>
#include <algorithm>
#include <regex>
#include "components/storage/conditional_expression.hpp"

namespace services::storage {

using components::storage::document_t;
using components::storage::conditional_expression;


template <class T>
class query_t : public conditional_expression {
    std::function<bool(T)> _check;
    std::string _key;

public:
    explicit query_t(std::string &&key)
        : _check([](T) { return false; })
        , _key(std::move(key))
    {}

    bool check(const document_t &doc) const override {
        return _check(doc.get_as<T>(_key));
    }

    query_t operator ==(const T &value) noexcept {
        return query_t(std::move(_key), [value](T v){ return v == value; });;
    }

    query_t operator !=(const T &value) noexcept {
        return query_t(std::move(_key), [value](T v){ return v != value; });
    }

    query_t operator <(const T &value) noexcept {
        return query_t(std::move(_key), [value](T v){ return v < value; });
    }

    query_t operator >(const T &value) noexcept {
        return query_t(std::move(_key), [value](T v){ return v > value; });
    }

    query_t operator <=(const T &value) noexcept {
        return query_t(std::move(_key), [value](T v){ return v <= value; });
    }

    query_t operator >=(const T &value) noexcept {
        return query_t(std::move(_key), [value](T v){ return v >= value; });
    }

    query_t between(const T &v1, const T &v2) {
        _check = [v1, v2](T v){ return v1 <= v && v <= v2; };
        return *this;
    }

    template <class CONT>
    query_t any(const CONT &array) {
        _check = [array](T v){ return std::find(array.begin(), array.end(), v) != array.end(); };
        return *this;
    }

    template <class CONT>
    query_t all(const CONT &array) {
        _check = [array](T v){ return std::find_if(array.begin(), array.end(), [&](T v2){ return v2 != v; }) == array.end(); };
        return *this;
    }

    query_t matches(const std::string &regex) {
        _check = [&](T v){ return std::regex_search(v, std::regex(regex)); };
        return *this;
    }

//    query_t operator &(const query_t &other) noexcept {
//        return query_t{_key, [&](T v){ return this->check(v) && other.check(v); }};
//    }

//    query_t operator |(const query_t &other) noexcept {
//        return query_t{[&](T v){ return this->check(v) || other.check(v); }};
//    }

//    query_t operator !() noexcept {
//        return query_t{[&](T v){ return !this->check(v); }};
//    }

private:
    query_t(std::string &&key, std::function<bool(T)> check)
        : _check(check)
        , _key(std::move(key))
    {}
};

}
