#pragma once

#include <components/ql/aggregate/limit.hpp>
#include <components/ql/statements/delete_many.hpp>
#include <components/ql/statements/delete_one.hpp>
#include "node.hpp"

namespace components::logical_plan {

    class node_limit_t final : public node_t {
    public:
        explicit node_limit_t(std::pmr::memory_resource* resource,
                              const collection_full_name_t& collection,
                              const components::ql::limit_t& limit);

        const components::ql::limit_t& limit() const;

    private:
        components::ql::limit_t limit_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };


    node_ptr make_node_limit(std::pmr::memory_resource* resource,
                             const collection_full_name_t& collection,
                             const components::ql::limit_t& limit);

} // namespace components::logical_plan
