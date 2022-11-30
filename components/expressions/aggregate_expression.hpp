#pragma once

#include "expression.hpp"
#include "key.hpp"

namespace components::expressions {

    class aggregate_expression_t;
    using aggregate_expression_ptr = boost::intrusive_ptr<aggregate_expression_t>;

    class aggregate_expression_t : public expression_i {
    public:
        using param_storage = std::variant<
            core::parameter_id_t,
            key_t,
            expression_ptr>;

        aggregate_expression_t(const aggregate_expression_t&) = delete;
        aggregate_expression_t(aggregate_expression_t&&) = default;

        aggregate_expression_t(aggregate_type type, const key_t& key);

        aggregate_type type() const;
        const key_t& key() const;
        const std::vector<param_storage>& params() const;

        void append_param(const param_storage& param);

    private:
        aggregate_type type_;
        key_t key_;
        std::vector<param_storage> params_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        bool equal_impl(const expression_i* rhs) const final;
    };

    aggregate_expression_ptr make_aggregate_expression(aggregate_type type, const key_t& key);
    aggregate_expression_ptr make_aggregate_expression(aggregate_type type);
    aggregate_expression_ptr make_aggregate_expression(aggregate_type type, const key_t& name, const key_t& key);

    aggregate_type get_aggregate_type(const std::string& key);
    bool is_aggregate_type(const std::string& key);

} // namespace components::expressions