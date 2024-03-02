#pragma once

#include "node.hpp"
#include <components/ql/join/join.hpp>

namespace components::logical_plan {

    class node_join_t final : public node_t {
    public:
        explicit node_join_t(std::pmr::memory_resource* resource,
                             const collection_full_name_t& collection,
                             ql::join_type type);

        ql::join_type type() const;

    private:
        ql::join_type type_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

} // namespace components::logical_plan
