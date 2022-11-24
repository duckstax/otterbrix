#include "expression.hpp"
#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    expression_type expression_i::type() const {
        return type_;
    }

    bool expression_i::is_scalar() const {
        return is_scalar_impl();
    }

    bool expression_i::is_aggregate() const {
        return is_aggregate_impl();
    }

    bool expression_i::is_window() const {
        return is_window_impl();
    }

    hash_t expression_i::hash() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, type_);
        boost::hash_combine(hash_, hash_impl());
        return hash_;
    }

    bool expression_i::operator==(const expression_i& rhs) const {
        return type_ == rhs.type_;
    }

    bool expression_i::operator!=(const expression_i& rhs) const {
        return !operator==(rhs);
    }

    expression_i::expression_i(expression_type type)
        : type_(type) {
    }

} // namespace components::logical_plan
