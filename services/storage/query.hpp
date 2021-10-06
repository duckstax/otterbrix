#pragma once
#include <functional>
#include <string>
#include <algorithm>
#include <regex>
#include <vector>
#include "components/storage/conditional_expression.hpp"

namespace services::storage {

using components::storage::document_t;
using components::storage::conditional_expression;


class empty_query_t : public conditional_expression {
    using query_ptr = std::unique_ptr<empty_query_t>;
public:
    explicit empty_query_t(std::string &&key);
    empty_query_t(std::string &&key, query_ptr &&q1, query_ptr &&q2 = nullptr);
    ~empty_query_t() override;
    bool check(const document_t &doc) const override;

    std::string _key;
    std::vector<empty_query_t *> _sub_query;
};

using query_ptr = std::unique_ptr<empty_query_t>;
query_ptr query(std::string &&key) noexcept;

query_ptr operator &(query_ptr &&q1, query_ptr &&q2) noexcept;
query_ptr operator |(query_ptr &&q1, query_ptr &&q2) noexcept;
query_ptr operator !(query_ptr &&q) noexcept;


template <class T>
class query_t : public empty_query_t {
public:
    explicit query_t(std::string &&key) //delete
        : empty_query_t(std::move(key))
        , _check([](T){ return true; })
    {}

    query_t(std::string &&key, std::function<bool(T)> check)
        : empty_query_t(std::move(key))
        , _check(check)
    {}

    bool check(const document_t &doc) const override {
        //todo
        return _check(doc.get_as<T>(_key));
    }

    query_t between(const T &v1, const T &v2) { //move
        _check = [v1, v2](T v){ return v1 <= v && v <= v2; };
        return *this;
    }

    template <class CONT>
    query_t any(const CONT &array) { //move
        _check = [array](T v){ return std::find(array.begin(), array.end(), v) != array.end(); };
        return *this;
    }

    template <class CONT>
    query_t all(const CONT &array) { //move
        _check = [array](T v){ return std::find_if(array.begin(), array.end(), [&](T v2){ return v2 != v; }) == array.end(); };
        return *this;
    }

    query_t matches(const std::string &regex) { //move
        _check = [&](T v){ return std::regex_search(v, std::regex(regex)); };
        return *this;
    }

private:
    std::function<bool(T)> _check;
};


template <class T>
query_ptr operator ==(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<T>(std::move(q->_key), [value](T v){ return v == value; }));
}

template <class T>
query_ptr operator !=(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<T>(std::move(q->_key), [value](T v){ return v != value; }));
}

template <class T>
query_ptr operator >(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<T>(std::move(q->_key), [value](T v){ return v > value; }));
}

template <class T>
query_ptr operator >=(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<T>(std::move(q->_key), [value](T v){ return v >= value; }));
}

template <class T>
query_ptr operator <(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<T>(std::move(q->_key), [value](T v){ return v < value; }));
}

template <class T>
query_ptr operator <=(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<T>(std::move(q->_key), [value](T v){ return v <= value; }));
}

}
