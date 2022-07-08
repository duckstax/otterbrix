#include <string>
#include <utility>
#include <vector>

#include "ql_statement.hpp"

enum class condition_type : std::uint8_t {
    novalid,
    eq,
    ne,
    gt,
    lt,
    gte,
    lte,
    regex,
    any,
    all,
    union_and,
    union_or,
    union_not
};

using ::document::impl::array_t;
using ::document::impl::value_t;
using ::document::impl::value_type;

template<class T>
struct find_condition_eq {
    std::string key_;
    std::string full_key_;
    T value_;
};

template<class T>
struct find_condition_ne {
    std::string key_;
    std::string full_key_;
    T value_;
};

template<class T>
struct find_condition_gt {
    std::string key_;
    std::string full_key_;
    T value_;
};

template<class T>
struct find_condition_lt {
    std::string key_;
    std::string full_key_;
    T value_;
};

template<class T>
struct find_condition_gte {
    std::string key_;
    std::string full_key_;
    T value_;
};

template<class T>
struct find_condition_lte {
    std::string key_;
    std::string full_key_;
    T value_;
};

template<class T>
struct find_condition_between {
    std::string key_;
    std::string full_key_;
    T value_;
    T value2_;
};

struct find_condition_regex {
    std::string key_;
    std::string full_key_;
    std::string value_;
};

/// ARRAY VALUES ///

template<class T>
struct find_condition_any {
    std::string key_;
    std::vector<T> values_;
};

template<class T>
struct find_condition_all {
    std::string key_;
    std::vector<T> values_;
};

/// COMPLEX VALUES ///

struct find_condition_and {
    std::string key_;
    std::string full_key_;
    std::vector<conditional_expression_ptr> conditions_;
};

struct find_condition_or {
    std::string key_;
    std::string full_key_;
    std::vector<conditional_expression_ptr> conditions_;
};

struct find_condition_not {
    std::string key_;
    std::string full_key_;
    std::vector<conditional_expression_ptr> conditions_;
};

///////////////////////////////////////////////

struct find_statement : public  ql_statement_t {
    std::vector<find_statement*> exps_;
};

conditional_expression_ptr make_find_condition(condition_type type, const std::string& key, const value_t* value);