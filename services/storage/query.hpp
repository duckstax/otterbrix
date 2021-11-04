#pragma once
#include <functional>
#include <string>
#include <algorithm>
#include <regex>
#include <vector>
#include "components/document/conditional_expression.hpp"

namespace services::storage {

using components::document::document_t;
using components::document::document_view_t;
using components::document::conditional_expression;


class empty_query_t : public conditional_expression {
    using query_ptr = std::unique_ptr<empty_query_t>;
public:
    explicit empty_query_t(std::string &&key);
    explicit empty_query_t(const std::string &key);
    empty_query_t(std::string &&key, query_ptr &&q1, query_ptr &&q2 = nullptr);
    ~empty_query_t() override;
    bool check(const document_view_t &doc) const override;
    bool check(const document_t &doc) const override;

    std::string key_;
    std::vector<empty_query_t *> sub_query_;
};

using query_ptr = std::unique_ptr<empty_query_t>;
query_ptr query(std::string &&key) noexcept; //delete

query_ptr operator &(query_ptr &&q1, query_ptr &&q2) noexcept;
query_ptr operator |(query_ptr &&q1, query_ptr &&q2) noexcept;
query_ptr operator !(query_ptr &&q) noexcept;


template<class T> struct query_helper               { typedef T type; };
template<> struct query_helper<char *>              { typedef std::string type; };
template<int size> struct query_helper<char [size]> { typedef std::string type; };
template<> struct query_helper<int>                 { typedef long type; };
template<> struct query_helper<uint>                { typedef ulong type; };
#define QH(T) typename query_helper<T>::type

template <class T>
class query_t : public empty_query_t {
public:
    query_t(const std::string &key, std::function<bool(QH(T))> check)
        : empty_query_t(key)
        , check_(check)
    {}

    bool check(const document_view_t &doc) const override {
        return check_(doc.get_as<QH(T)>(key_));
    }

    bool check(const document_t &doc) const override {
        return check_(doc.get_as<QH(T)>(key_));
    }

private:
    std::function<bool(QH(T))> check_;
};


template <class T> //delete
query_ptr operator ==(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(std::move(q->key_), [value](QH(T) v){ return v == value; }));
}

template <class T> //delete
query_ptr operator !=(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(std::move(q->key_), [value](QH(T) v){ return v != value; }));
}

template <class T> //delete
query_ptr operator >(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(std::move(q->key_), [value](QH(T) v){ return v > value; }));
}

template <class T> //delete
query_ptr operator >=(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(std::move(q->key_), [value](QH(T) v){ return v >= value; }));
}

template <class T> //delete
query_ptr operator <(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(std::move(q->key_), [value](QH(T) v){ return v < value; }));
}

template <class T> //delete
query_ptr operator <=(query_ptr &&q, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(std::move(q->key_), [value](QH(T) v){ return v <= value; }));
}


template <class T>
query_ptr eq(const std::string &key, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(key, [value](QH(T) v){ return v == value; }));
}

template <class T>
query_ptr ne(const std::string &key, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(key, [value](QH(T) v){ return v != value; }));
}

template <class T>
query_ptr gt(const std::string &key, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(key, [value](QH(T) v){ return v > value; }));
}

template <class T>
query_ptr gte(const std::string &key, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(key, [value](QH(T) v){ return v >= value; }));
}

template <class T>
query_ptr lt(const std::string &key, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(key, [value](QH(T) v){ return v < value; }));
}

template <class T>
query_ptr lte(const std::string &key, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(key, [value](QH(T) v){ return v <= value; }));
}


template <class T>
query_ptr between(const std::string &key, const T &v1, const T &v2) {
    return query_ptr(new query_t<QH(T)>(key, [v1, v2](QH(T) v){
        return v1 <= v && v <= v2;
    }));
}

template <class CONT>
query_ptr any(const std::string &key, const CONT &array) {
    return query_ptr(new query_t<typename CONT::value_type>(key, [array](typename CONT::value_type v){
        return std::find(array.begin(), array.end(), v) != array.end();
    }));
}

template <class CONT>
query_ptr all(const std::string &key, const CONT &array) {
    return query_ptr(new query_t<typename CONT::value_type>(key, [array](typename CONT::value_type v){
        return std::find_if(array.begin(), array.end(), [&](typename CONT::value_type v2){ return v2 != v; }) == array.end();
    }));
}

query_ptr any(const std::string &key, const ::document::impl::array_t *array);
query_ptr all(const std::string &key, const ::document::impl::array_t *array);
query_ptr matches(const std::string &key, const std::string &regex);

query_ptr parse_condition(const document_t &cond, query_ptr &&prev_cond = nullptr, const std::string &prev_key = "");

}
