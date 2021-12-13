#pragma once
#include <functional>
#include <string>
#include <algorithm>
#include <regex>
#include <vector>
#include <cmath>
#include "components/document/conditional_expression.hpp"

namespace services::storage {

using components::document::document_t;
using components::document::document_view_t;
using components::document::conditional_expression;


enum class query_type {
    simple,
    any,
    all
};


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


template<class T> struct query_helper                { typedef T type; };
template<> struct query_helper<char *>               { typedef std::string type; };
template<uint size> struct query_helper<char [size]> { typedef std::string type; };
template<> struct query_helper<int>                  { typedef long type; };
template<> struct query_helper<uint>                 { typedef ulong type; };
#define QH(T) typename query_helper<T>::type

template<class T> inline bool query_equals(T v1, T v2) {
    return v1 == v2;
}
template<> inline bool query_equals<double>(double v1, double v2) {
    return std::fabs(v1 - v2) < std::numeric_limits<double>::epsilon();
}

template <class T>
class query_t : public empty_query_t {
public:
    query_t(const std::string &key, std::function<bool(QH(T))> check, query_type type = query_type::simple)
        : empty_query_t(key)
        , type_(type)
        , check_(check)
    {}

    bool check(const document_view_t &doc) const override {
        document_view_t sub_doc(doc);
        std::size_t start = 0;
        std::size_t dot_pos = key_.find(".");
        while (dot_pos != std::string::npos) {
            auto key = key_.substr(start, dot_pos - start);
            if (sub_doc.is_dict(key)) sub_doc = sub_doc.get_dict(key);
            else if (sub_doc.is_array(key)) sub_doc = sub_doc.get_array(key);
            else return false;
            start = dot_pos + 1;
            dot_pos = key_.find(".", start);
        }
        auto key = key_.substr(start, key_.size() - start);
        if (sub_doc.is_array(key)) {
            return check_array_(sub_doc.get_array(key));
        }
        return check_(sub_doc.get_as<QH(T)>(key));
    }

    bool check(const document_t &doc) const override {
        if (doc.is_array(key_)) {
            return check_array_(doc.get_array(key_));
        }
        return check_(doc.get_as<QH(T)>(key_));
    }

private:
    query_type type_;
    std::function<bool(QH(T))> check_;

    template<class TAr>
    bool check_array_(const TAr &array) const {
        switch (type_) {
        case query_type::simple: return true;
        case query_type::any: return check_array_any_(array);
        case query_type::all: return check_array_all_(array);
        }
        return false;
    }

    bool check_array_any_(const document_view_t &array) const {
        for (uint32_t i = 0; i < array.count(); ++i) {
            if (array.is_array(i)) {
                if (check_array_any_(array.get_array(i))) {
                    return true;
                }
            } else {
                if (check_(array.get_as<QH(T)>(i))) {
                    return true;
                }
            }
        }
        return false;
    }
    bool check_array_any_(const ::document::impl::array_t *array) const {
        for (auto it = array->begin(); it; ++it) {
            if (check_(it.value()->as<QH(T)>())) {
                return true;
            }
        }
        return false;
    }

    bool check_array_all_(const document_view_t &array) const {
        for (uint32_t i = 0; i < array.count(); ++i) {
            if (array.is_array(i)) {
                if (!check_array_any_(array.get_array(i))) {
                    return false;
                }
            } else {
                if (!check_(array.get_as<QH(T)>(i))) {
                    return false;
                }
            }
        }
        return true;
    }
    bool check_array_all_(const ::document::impl::array_t *array) const {
        for (auto it = array->begin(); it; ++it) {
            if (!check_(it.value()->as<QH(T)>())) {
                return false;
            }
        }
        return true;
    }
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
    return query_ptr(new query_t<QH(T)>(key, [value](QH(T) v){ return query_equals<QH(T)>(v, value); }));
}

template <class T>
query_ptr ne(const std::string &key, const T &value) noexcept {
    return query_ptr(new query_t<QH(T)>(key, [value](QH(T) v){ return !query_equals<QH(T)>(v, value); }));
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
query_ptr contains(const std::string &key, const CONT &array, query_type type) {
    return query_ptr(new query_t<typename CONT::value_type>(key, [array](typename CONT::value_type v){
        return std::find(array.begin(), array.end(), v) != array.end();
    }, type));
}

template <class CONT>
query_ptr any(const std::string &key, const CONT &array) {
    return contains<CONT>(key, array, query_type::any);
}

template <class CONT>
query_ptr all(const std::string &key, const CONT &array) {
    return contains<CONT>(key, array, query_type::all);
}

query_ptr contains(const std::string &key, const ::document::impl::array_t *array, query_type type);
query_ptr any(const std::string &key, const ::document::impl::array_t *array);
query_ptr all(const std::string &key, const ::document::impl::array_t *array);
query_ptr matches(const std::string &key, const std::string &regex);

query_ptr parse_condition(const document_t &cond, query_ptr &&prev_cond = nullptr, const std::string &prev_key = "");

}
