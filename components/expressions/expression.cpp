#include "expression.hpp"

#include "aggregate_expression.hpp"
#include "compare_expression.hpp"
#include "scalar_expression.hpp"
#include "sort_expression.hpp"

#include <boost/container_hash/hash.hpp>
#include <components/serialization/deserializer.hpp>
#include <sstream>

namespace components::expressions {

    expression_group expression_i::group() const { return group_; }

    hash_t expression_i::hash() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, group_);
        boost::hash_combine(hash_, hash_impl());
        return hash_;
    }

    std::string expression_i::to_string() const { return to_string_impl(); }

    bool expression_i::operator==(const expression_i& rhs) const { return group_ == rhs.group_ && equal_impl(&rhs); }

    bool expression_i::operator!=(const expression_i& rhs) const { return !operator==(rhs); }

    void expression_i::serialize(serializer::base_serializer_t* serializer) const { return serialize_impl(serializer); }

    boost::intrusive_ptr<expression_i> expression_i::deserialize(serializer::base_deserializer_t* deserializer) {
        auto type = deserializer->current_type();
        switch (type) {
            case serializer::serialization_type::expression_compare:
                return compare_expression_t::deserialize(deserializer);
            case serializer::serialization_type::expression_aggregate:
                return aggregate_expression_t::deserialize(deserializer);
            case serializer::serialization_type::expression_scalar:
                return scalar_expression_t::deserialize(deserializer);
            case serializer::serialization_type::expression_sort:
                return sort_expression_t::deserialize(deserializer);
            default:
                return {nullptr};
        }
    }

    expression_i::expression_i(expression_group group)
        : group_(group) {}

} // namespace components::expressions
