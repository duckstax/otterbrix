#include "conditional_expression.hpp"

namespace components::parser {

conditional_expression::conditional_expression(const std::string &key)
    : full_key_(key) {
}

bool conditional_expression::is_union() const {
    return false;
}

void conditional_expression::set_value(const value_t *) {}

const document_view_t conditional_expression::prepare_document_(const document_view_t &doc) {
    document_view_t res(doc);
    std::size_t start = 0;
    key_ = full_key_;
    std::size_t dot_pos = key_.find(".");
    while (dot_pos != std::string::npos) {
        auto key = key_.substr(start, dot_pos - start);
        if (res.is_dict(key)) res = res.get_dict(key);
        else if (res.is_array(key)) res = res.get_array(key);
        else return document_view_t();
        start = dot_pos + 1;
        dot_pos = key_.find(".", start);
    }
    key_ = key_.substr(start, key_.size() - start);
    return res;
}

const document_t conditional_expression::prepare_document_(const document_t &doc) {
    key_ = full_key_;
    return doc;
}


find_condition_t::find_condition_t(std::vector<conditional_expression_ptr> &&conditions)
    : conditional_expression(std::string())
    , conditions_(std::move(conditions)) {}

find_condition_t::find_condition_t(const std::vector<conditional_expression_ptr> &conditions)
    : conditional_expression(std::string())
    , conditions_(conditions) {}

void find_condition_t::add(conditional_expression_ptr &&condition) {
    if (condition) {
        conditions_.push_back(std::move(condition));
    }
}

bool find_condition_t::check_document(const document_view_t &doc) const {
    return check_document_(doc);
}

bool find_condition_t::check_document(const document_t &doc) const {
    return check_document_(doc);
}

bool find_condition_t::is_union() const {
    return true;
}

template<class T> bool find_condition_t::check_document_(const T &doc) const {
    for (const auto &condition : conditions_) {
        if (!condition->is_fit(doc)) {
            return false;
        }
    }
    return true;
}

}
