#pragma once

#include "node.hpp"
#include <components/ql/index.hpp>
#include <memory>

namespace components::logical_plan {

    class node_drop_index_t final : public node_t {
    public:
        explicit node_drop_index_t(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   components::ql::drop_index_t* ql);

        components::ql::drop_index_t* get_ql() const;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        components::ql::drop_index_t* ql_;
    };

    node_ptr make_node_drop_index(std::pmr::memory_resource* resource, components::ql::drop_index_t* ql);

} // namespace components::logical_plan
