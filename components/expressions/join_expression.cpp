#include "join_expression.hpp"
#include <boost/container_hash/hash.hpp>
#include <sstream>

namespace components::expressions {

    struct join_expression_field_hash {
        inline std::size_t operator()(const join_expression_field& f) const {
            hash_t hash_{0};
            boost::hash_combine(hash_, collection_name_hash()(f.collection));
            boost::hash_combine(hash_, f.expr->hash());
            return hash_;
        }
    };

    bool operator==(const join_expression_field& f1, const join_expression_field& f2) {
        return f1.collection == f2.collection && *f1.expr == *f2.expr;
    }

    join_expression_t::join_expression_t(std::pmr::memory_resource*,
                                         compare_type compare,
                                         join_expression_field left,
                                         join_expression_field right)
        : expression_i(expression_group::join)
        , compare_(compare)
        , left_(std::move(left))
        , right_(std::move(right)) {}

    compare_type join_expression_t::compare() const { return compare_; }

    const join_expression_field& join_expression_t::left() const { return left_; }

    const join_expression_field& join_expression_t::right() const { return right_; }

    hash_t join_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, compare_);
        boost::hash_combine(hash_, join_expression_field_hash()(left_));
        boost::hash_combine(hash_, join_expression_field_hash()(right_));
        return hash_;
    }

    std::string join_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << compare_ << ": [";
        stream << left_.collection.to_string() << "." << left_.expr->to_string();
        stream << ", ";
        stream << right_.collection.to_string() << "." << right_.expr->to_string();
        stream << "]";
        return stream.str();
    }

    bool join_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const join_expression_t*>(rhs);
        return compare_ == other->compare_ && left_ == left_ && right_ == right_;
    }

    join_expression_ptr make_join_expression(std::pmr::memory_resource* resource,
                                             compare_type compare,
                                             join_expression_field left,
                                             join_expression_field right) {
        return new join_expression_t(resource, compare, std::move(left), std::move(right));
    }

} // namespace components::expressions
