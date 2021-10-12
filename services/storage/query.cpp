#include "query.hpp"

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

query_ptr matches(const std::string &key, const std::string &regex) {
    return query_ptr(new query_t<std::string>(std::move(key), [regex](std::string v){
                         return std::regex_match(v, std::regex(regex));
                     }));
}


#define GET_CONDITION(FUNC, KEY, DOC) \
    DOC.is_boolean() ? FUNC(KEY, DOC.as_bool()) : \
    DOC.is_integer() ? FUNC(KEY, DOC.as_int32()) : \
    DOC.is_float()   ? FUNC(KEY, DOC.as_double()) : \
    DOC.is_string()  ? FUNC(KEY, DOC.as_string()) : \
    nullptr; //todo

query_ptr parse_condition(const document_t &cond, query_ptr &&prev_cond, const std::string &prev_key) {
    query_ptr q = nullptr;
//    std::cerr << cond.to_string() << std::endl;
    for (auto it = cond.cbegin(); it != cond.cend(); ++it) {
        if (it.key() == "$eq") {
            document_t doc(it.value());
            query_ptr q2 = GET_CONDITION(eq, prev_key, doc);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (it.key() == "$ne") {
            document_t doc(it.value());
            query_ptr q2 = GET_CONDITION(ne, prev_key, doc);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (it.key() == "$gt") {
            document_t doc(it.value());
            query_ptr q2 = GET_CONDITION(gt, prev_key, doc);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (it.key() == "$gte") {
            document_t doc(it.value());
            query_ptr q2 = GET_CONDITION(gte, prev_key, doc);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (it.key() == "$lt") {
            document_t doc(it.value());
            query_ptr q2 = GET_CONDITION(lt, prev_key, doc);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (it.key() == "$lte") {
            document_t doc(it.value());
            query_ptr q2 = GET_CONDITION(lte, prev_key, doc);
            q = q ? std::move(q) & std::move(q2) : std::move(q2);
        } else if (it.key() == "$and") {
            q = query("and");
            for (auto &val : it.value()) {
                q->sub_query_.push_back(parse_condition(val).release());
            }
        } else if (it.key() == "$or") {
            q = query("or");
            for (auto &val : it.value()) {
                q->sub_query_.push_back(parse_condition(val).release());
            }
        } else if (it.key() == "$not") {
            q = query("not");
            q->sub_query_.push_back(parse_condition(it.value()).release());
        } else if (it.key() == "$in") {
            q = any(prev_key, it.value());
        } else if (it.key() == "$all") {
            q = all(prev_key, it.value());
        } else if (it.key() == "$regex") {
            std::string regex = document_t(it.value()).as_string();
            //regex = std::regex_replace(regex, std::regex("\\\\\\"), "|||");
            //regex = std::regex_replace(regex, std::regex("\\\\"), "|||");
            //regex = std::regex_replace(regex, std::regex("\\"), "");
            //regex = std::regex_replace(regex, std::regex("|||"), "\\");
            q = matches(prev_key, regex);
        } else if (it.value().is_object()) {
            return parse_condition(it.value(), std::move(q), it.key());
        }
    }
    return prev_cond ? std::move(prev_cond) & std::move(q) : std::move(q);
}

}
