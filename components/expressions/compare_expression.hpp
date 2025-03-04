#pragma once

#include "expression.hpp"
#include "key.hpp"
#include <memory>
#include <memory_resource>
#include <msgpack.hpp>
#include <msgpack/adaptor/list.hpp>
#include <msgpack/zone.hpp>

namespace components::expressions {

    class compare_expression_t;
    using compare_expression_ptr = boost::intrusive_ptr<compare_expression_t>;

    class compare_expression_t : public expression_i {
    public:
        compare_expression_t(const compare_expression_t&) = delete;
        compare_expression_t(compare_expression_t&&) noexcept = default;
        ~compare_expression_t() final = default;

        compare_expression_t(std::pmr::memory_resource* resource,
                             compare_type type,
                             const key_t& key,
                             core::parameter_id_t);
        compare_expression_t(std::pmr::memory_resource* resource,
                             compare_type type,
                             const key_t& key_left,
                             const key_t& key_right);

        compare_type type() const;
        const key_t& key_left() const;
        const key_t& key_right() const;
        core::parameter_id_t value() const;
        const std::pmr::vector<compare_expression_ptr>& children() const;

        void set_type(compare_type type);
        void append_child(const compare_expression_ptr& child);

        bool is_union() const;

    private:
        compare_type type_;
        key_t key_left_;
        key_t key_right_;
        core::parameter_id_t value_;
        std::pmr::vector<compare_expression_ptr> children_;
        bool union_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        bool equal_impl(const expression_i* rhs) const final;
    };

    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource,
                                                   compare_type type,
                                                   const key_t& key,
                                                   core::parameter_id_t id);
    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource,
                                                   compare_type type,
                                                   const key_t& key_left,
                                                   const key_t& key_right);
    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource, compare_type type);
    compare_expression_ptr make_compare_union_expression(std::pmr::memory_resource* resource, compare_type type);

    bool is_union_compare_condition(compare_type type);
    compare_type get_compare_type(const std::string& key);

} // namespace components::expressions
