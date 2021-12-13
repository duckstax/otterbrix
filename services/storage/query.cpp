#include "query.hpp"
#include "components/document/core/dict.hpp"
#include "components/document/mutable/mutable_dict.h"

using ::document::impl::value_type;

namespace services::storage {

empty_query_t::empty_query_t(std::string &&key)
    : key_(std::move(key))
{}

empty_query_t::empty_query_t(const std::string &key)
    : key_(key)
{}

empty_query_t::empty_query_t(std::string &&key, query_ptr &&q1, query_ptr &&q2)
    : key_(std::move(key))
{
    sub_query_.push_back(std::move(q1.release()));
    if (q2) {
        sub_query_.push_back(std::move(q2.release()));
    }
}

empty_query_t::~empty_query_t() {
    for (auto q : sub_query_) {
        delete q;
    }
    sub_query_.clear();
}

bool empty_query_t::check(const components::document::document_view_t &doc) const {
    if (!sub_query_.empty()) {
        if (key_ == "and") {
            bool res = true;
            for (auto q : sub_query_) {
                res &= q->check(doc);
            }
            return res;
        } else if (key_ == "or") {
            bool res = false;
            for (auto q : sub_query_) {
                res |= q->check(doc);
            }
            return res;
        } else if (key_ == "not") {
            return !sub_query_.at(0)->check(doc);
        }
    }
    return true;
}

bool empty_query_t::check(const components::document::document_t &doc) const {
    if (!sub_query_.empty()) {
        if (key_ == "and") {
            bool res = true;
            for (auto q : sub_query_) {
                res &= q->check(doc);
            }
            return res;
        } else if (key_ == "or") {
            bool res = false;
            for (auto q : sub_query_) {
                res |= q->check(doc);
            }
            return res;
        } else if (key_ == "not") {
            return !sub_query_.at(0)->check(doc);
        }
    }
    return true;
}


query_ptr query(std::string &&key) noexcept {
    return std::make_unique<empty_query_t>(std::move(key));
}

query_ptr union_query(std::string &&key, query_ptr &&q1, query_ptr &&q2) noexcept {
    if (key == q1->key_) {
        q1->sub_query_.push_back(q2.release());
        return std::move(q1);
    }
    if (key == q2->key_) {
        q2->sub_query_.push_back(q1.release());
        return std::move(q2);
    }
    return std::make_unique<empty_query_t>(std::move(key), std::move(q1), std::move(q2));
}

query_ptr operator &(query_ptr &&q1, query_ptr &&q2) noexcept {
    return union_query("and", std::move(q1), std::move(q2));
}

query_ptr operator |(query_ptr &&q1, query_ptr &&q2) noexcept {
    return union_query("or", std::move(q1), std::move(q2));
}

query_ptr operator !(query_ptr &&q) noexcept {
    if (q->key_ == "not") {
        auto res = query_ptr(q->sub_query_.at(0));
        q->sub_query_.clear();
        return res;
    }
    return std::make_unique<empty_query_t>("not", std::move(q));
}


#define CONTAINS_RES(TYPE, CONVERT) \
    query_ptr(new query_t<TYPE>(key, [array](TYPE v){ \
        for (auto it = array->begin(); it; ++it) { \
            if (query_equals<QH(TYPE)>(static_cast<TYPE>(it.value()->CONVERT), v)) return true; \
        } \
        return false; \
    }, type))

query_ptr contains(const std::string &key, const ::document::impl::array_t *array, query_type type) {
    if (array->count() > 0) {
        auto value0 = array->get(0);
        if (value0->type() == value_type::boolean) {
            return CONTAINS_RES(bool, as_bool());
        } else if (value0->is_unsigned()) {
            return CONTAINS_RES(ulong, as_unsigned());
        } else if (value0->is_int()) {
            return CONTAINS_RES(long, as_int());
        } else if (value0->is_double()) {
            return CONTAINS_RES(double, as_double());
        } else if (value0->type() == value_type::string) {
            return CONTAINS_RES(std::string, as_string());
        }
    }
    return nullptr;
}

query_ptr any(const std::string &key, const ::document::impl::array_t *array) {
    return contains(key, array, query_type::any);
}

query_ptr all(const std::string &key, const ::document::impl::array_t *array) {
    return contains(key, array, query_type::all);
}

query_ptr matches(const std::string &key, const std::string &regex) {
    return query_ptr(new query_t<std::string>(std::move(key), [regex](std::string v){
                         return std::regex_match(v, std::regex(".*" + regex + ".*"));
                     }));
}


#define GET_CONDITION(FUNC, KEY, VALUE) \
    (value->type() == value_type::boolean) ? FUNC(KEY, VALUE->as_bool()) : \
    (value->is_unsigned()) ? FUNC(KEY, VALUE->as_unsigned()) : \
    (value->is_int()) ? FUNC(KEY, VALUE->as_int()) : \
    (value->is_double()) ? FUNC(KEY, VALUE->as_double()) : \
    (value->type() == value_type::string) ? FUNC(KEY, static_cast<std::string>(VALUE->as_string())) : \
    nullptr;

query_ptr parse_condition(const document_t &cond, query_ptr &&prev_cond, const std::string &prev_key) {
    query_ptr q = nullptr;
    for (auto it = cond.begin(); it; ++it) {
        auto key = static_cast<std::string>(it.key()->as_string());
        auto value = it.value();
        if (key == "$eq") {
            query_ptr q2 = GET_CONDITION(eq, prev_key, value);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (key == "$ne") {
            query_ptr q2 = GET_CONDITION(ne, prev_key, value);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (key == "$gt") {
            query_ptr q2 = GET_CONDITION(gt, prev_key, value);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (key == "$gte") {
            query_ptr q2 = GET_CONDITION(gte, prev_key, value);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (key == "$lt") {
            query_ptr q2 = GET_CONDITION(lt, prev_key, value);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (key == "$lte") {
            query_ptr q2 = GET_CONDITION(lte, prev_key, value);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (key == "$and") {
            q = query("and");
            for (auto it = value->as_array()->begin(); it; ++it) {
                auto dict = ::document::impl::mutable_dict_t::new_dict(it.value()->as_dict()).detach();
                q->sub_query_.push_back(parse_condition(document_t(dict, true)).release());
            }
        } else if (key == "$or") {
            q = query("or");
            for (auto it = value->as_array()->begin(); it; ++it) {
                auto dict = ::document::impl::mutable_dict_t::new_dict(it.value()->as_dict()).detach();
                q->sub_query_.push_back(parse_condition(document_t(dict, true)).release());
            }
        } else if (key == "$not") {
            q = query("not");
            auto dict = ::document::impl::mutable_dict_t::new_dict(it.value()->as_dict()).detach();
            q->sub_query_.push_back(parse_condition(document_t(dict, true)).release());
        } else if (key == "$in") {
            q = any(prev_key, value->as_array());
        } else if (key == "$all") {
            q = all(prev_key, value->as_array());
        } else if (key == "$regex") {
            auto regex = static_cast<std::string>(value->as_string());
            q = matches(prev_key, regex);
        } else if (value->type() == value_type::dict) {
            auto dict = ::document::impl::mutable_dict_t::new_dict(value->as_dict()).detach();
            return parse_condition(document_t(dict, true), std::move(q), key);
        } else {
            query_ptr q2 = GET_CONDITION(eq, key, value);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        }
    }
    return prev_cond
            ? std::move(prev_cond) & std::move(q)
            : std::move(q);
}

}
