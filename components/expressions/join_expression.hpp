#pragma once

#include "expression.hpp"
#include "key.hpp"
#include "scalar_expression.hpp"
#include <components/base/collection_full_name.hpp>
#include <memory>
#include <memory_resource>

namespace components::expressions {

    struct join_expression_field {
        collection_full_name_t collection;
        scalar_expression_ptr expr;
    };

    class join_expression_t final : public expression_i {
    public:
        join_expression_t(const join_expression_t&) = delete;
        join_expression_t(join_expression_t&&) = default;
        ~join_expression_t() final = default;

        join_expression_t(std::pmr::memory_resource* resource,
                          compare_type compare,
                          join_expression_field left,
                          join_expression_field right);

        compare_type compare() const;
        const join_expression_field& left() const;
        const join_expression_field& right() const;

    private:
        compare_type compare_;
        join_expression_field left_;
        join_expression_field right_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        bool equal_impl(const expression_i* rhs) const final;
    };

    using join_expression_ptr = boost::intrusive_ptr<join_expression_t>;

    join_expression_ptr make_join_expression(std::pmr::memory_resource* resource,
                                             compare_type compare,
                                             join_expression_field left,
                                             join_expression_field right);

} // namespace components::expressions
