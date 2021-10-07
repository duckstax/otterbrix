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

query_ptr operator &(query_ptr &&q1, query_ptr &&q2) noexcept {
    return std::make_unique<empty_query_t>("and", std::move(q1), std::move(q2));
}

query_ptr operator |(query_ptr &&q1, query_ptr &&q2) noexcept {
    return std::make_unique<empty_query_t>("or", std::move(q1), std::move(q2));
}

query_ptr operator !(query_ptr &&q) noexcept {
    return std::make_unique<empty_query_t>("not", std::move(q));
}

query_ptr matches(query_ptr &&q, const std::string &regex) {
    return query_ptr(new query_t<std::string>(std::move(q->key_), [&](std::string v){
                         return std::regex_search(v, std::regex(regex));
                     }));
}

query_ptr matches(std::string &&key, const std::string &regex) {
    return matches(query(std::move(key)), regex);
}

}
