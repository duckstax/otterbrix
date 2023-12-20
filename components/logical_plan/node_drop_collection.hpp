#pragma once

#include "node.hpp"
#include <components/ql/statements/drop_collection.hpp>

namespace components::logical_plan {

    class node_drop_collection_t final : public node_t {
    public:
        explicit node_drop_collection_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    node_ptr make_node_drop_collection(std::pmr::memory_resource* resource, ql::drop_collection_t* ql);

} // namespace components::logical_plan
