#include "ql_statement.hpp"
#include "expr.hpp"

template<class T>
bool condition_eq(T v1, T v2) {
    return v1 == v2;
}

template<>
bool condition_eq<double>(double v1, double v2) {
    return std::fabs(v1 - v2) < std::numeric_limits<double>::epsilon();
}

template<class T>
class find_condition_eq_ne_gt_lt_gte_lte final : public expr_t {
protected:
     bool eq_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

     bool ne_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

     bool gt_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

     bool lt_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

     bool gte_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

     bool lte_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

private:
    std::string key_;
    std::string full_key_;
    T value_;
};


template<class T>
struct find_condition_between final : public condition_t {
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
struct find_condition_any final : public condition_t {
    std::string key_;
    std::vector<T> values_;
};

template<class T>
struct find_condition_all final : public condition_t {
    std::string key_;
    std::vector<T> values_;
};

/// COMPLEX VALUES ///

struct find_condition_and final : public condition_t {
    std::string key_;
    std::string full_key_;
    std::vector<condition_ptr> conditions_;
};

struct find_condition_or final : public condition_t {
    std::string key_;
    std::string full_key_;
    std::vector<condition_ptr> conditions_;
};

struct find_condition_not final : public condition_t {
    condition_type  condition_;
    std::string key_;
    std::string full_key_;
    std::vector<condition_ptr> conditions_;
};

///////////////////////////////////////////////

struct find_statement : public ql_statement_t {
    std::vector<expr_ptr> condition_;
};

condition_ptr make_find_condition(condition_type type, const std::string& key, const value_t* value);