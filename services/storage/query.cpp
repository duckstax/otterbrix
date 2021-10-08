#include "query.hpp"

namespace services::storage {

empty_query_t::empty_query_t(std::string &&key)
    : key_(std::move(key))
{}

empty_query_t::empty_query_t(std::string &&key, query_ptr &&q1, query_ptr &&q2)
    : key_(std::move(key))
{
    sub_query_.push_back(std::move(q1.release()));
    if (q2) sub_query_.push_back(std::move(q2.release()));
}

empty_query_t::~empty_query_t() {
    for (auto q : sub_query_) {
        delete q;
    }
    sub_query_.clear();
}

bool empty_query_t::check(const document_t &doc) const {
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

query_ptr matches(std::string &&key, const std::string &regex) {
    return query_ptr(new query_t<std::string>(std::move(key), [&](std::string v){
                         return std::regex_search(v, std::regex(regex));
                     }));
}

}
