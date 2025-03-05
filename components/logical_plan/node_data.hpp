#pragma once

#include "node.hpp"
#include <components/ql/statements/raw_data.hpp>

namespace components::logical_plan {

    class node_data_t final : public node_t {
    public:
        explicit node_data_t(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::document::document_ptr>&& documents);

        const std::pmr::vector<components::document::document_ptr>& documents() const;

    private:
        std::pmr::vector<components::document::document_ptr> documents_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    node_ptr make_node_data(std::pmr::memory_resource* resource, ql::raw_data_t* data);

} // namespace components::logical_plan
