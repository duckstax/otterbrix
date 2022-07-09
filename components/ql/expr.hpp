#pragma once

#include <string>
#include <vector>
#include <variant>
#include <utility>

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

using storage_t = std::variant<bool, uint64_t, int64_t, double, std::string>;

struct expr_t {
public:
    bool operator==(const expr_t& other) {
        return eq_impl(other);
    }
    bool operator!=(const expr_t& other) {
        return ne_impl(other);
    }
    bool operator<(const expr_t& other) {
        return lt_impl(other);
    }
    bool operator>(const expr_t& other) {
        return gt_impl(other);
    }
    bool operator<=(const expr_t& other) {
        return lte_impl(other);
    }
    bool operator>=(const expr_t& other) {
        return gte_impl(other);
    }

protected:
    condition_type  condition_;
    virtual bool eq_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

    virtual bool ne_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

    virtual bool gt_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

    virtual bool lt_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

    virtual bool gte_impl(const condition_t& other) {
        throw std::runtime_error("");
    }

    virtual bool lte_impl(const condition_t& other) {
        throw std::runtime_error("");
    }
};


using expr_ptr = expr_t*;

make_condition