#include "query.hpp"

namespace services::storage {

empty_query_t::empty_query_t(std::string &&key)
    : _key(std::move(key))
{}

empty_query_t::empty_query_t(std::string &&key, query_ptr &&q1, query_ptr &&q2)
    : _key(std::move(key))
{
    _sub_query.push_back(std::move(q1.release()));
    if (q2) _sub_query.push_back(std::move(q2.release()));
}

empty_query_t::~empty_query_t() {
    for (auto q : _sub_query) {
        delete q;
    }
    _sub_query.clear();
}

bool empty_query_t::check(const document_t &doc) const {
    if (!_sub_query.empty()) {
        if (_key == "and") {
            bool res = true;
            for (auto q : _sub_query) {
                res &= q->check(doc);
            }
            return res;
        } else if (_key == "or") {
            bool res = false;
            for (auto q : _sub_query) {
                res |= q->check(doc);
            }
            return res;
        } else if (_key == "not") {
            return !_sub_query.at(0)->check(doc);
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

}
