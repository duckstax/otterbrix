#pragma once

#include "node.hpp"
#include <components/ql/index.hpp>
#include <memory>

namespace components::logical_plan {

    class node_create_index_t final : public node_t {
    public:
        explicit node_create_index_t(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::ql::create_index_t* ql);

        std::unique_ptr<components::ql::create_index_t> get_ql() const;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        mutable std::unique_ptr<components::ql::create_index_t> ql_;
    };

    node_ptr make_node_create_index(std::pmr::memory_resource* resource, components::ql::create_index_t* ql);

} // namespace components::logical_plan
