#pragma once
#include <vector>
#include <functional>
#include <string>
#include <algorithm>
#include <regex>
#include <iostream>

namespace services::storage {

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
        return query_t([&](VALUE v){ std::cout << v << " " << value << std::endl; return v == value; });
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

    static query_t matches(const std::string &regex) {
        return query_t([&](VALUE v){
            return std::regex_search(v, std::regex(regex));
        });
    }
};

}
