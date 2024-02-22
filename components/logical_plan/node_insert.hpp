#pragma once

#include "node.hpp"
#include <components/ql/statements/insert_many.hpp>
#include <components/ql/statements/insert_one.hpp>

namespace components::logical_plan {

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               std::pmr::vector<components::document::document_ptr>&& documents);

        const std::pmr::vector<components::document::document_ptr>& documents() const;

    private:
        std::pmr::vector<components::document::document_ptr> documents_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    node_ptr make_node_insert(std::pmr::memory_resource* resource, ql::insert_many_t* insert);

    node_ptr make_node_insert(std::pmr::memory_resource* resource, ql::insert_one_t* insert);

} // namespace components::logical_plan
