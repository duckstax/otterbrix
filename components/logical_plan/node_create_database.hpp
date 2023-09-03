#pragma once

#include <components/ql/statements/create_database.hpp>
#include "node.hpp"

namespace components::logical_plan {

    class node_create_database_t final : public node_t {
    public:
        explicit node_create_database_t(std::pmr::memory_resource* resource,
                                        const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };


    node_ptr make_node_create_database(std::pmr::memory_resource* resource,
                                       ql::create_database_t* ql);

} // namespace components::logical_plan
