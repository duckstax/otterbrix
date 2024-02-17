#pragma once

#include "node.hpp"
#include <components/ql/aggregate/limit.hpp>
#include <components/ql/statements/update_many.hpp>
#include <components/ql/statements/update_one.hpp>

namespace components::logical_plan {

    class node_update_t final : public node_t {
    public:
        explicit node_update_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               const components::ql::aggregate::match_t& match,
                               const components::ql::limit_t& limit,
                               const components::document::document_ptr& update,
                               bool upsert);

        const components::document::document_ptr& update() const;
        bool upsert() const;

    private:
        components::document::document_ptr update_;
        bool upsert_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    node_ptr make_node_update(std::pmr::memory_resource* resource, ql::update_many_t* ql);

    node_ptr make_node_update(std::pmr::memory_resource* resource, ql::update_one_t* ql);

} // namespace components::logical_plan
