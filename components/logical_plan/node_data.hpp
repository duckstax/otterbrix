#pragma once

#include "node.hpp"

#include <components/document/document.hpp>

namespace components::logical_plan {

    class node_data_t final : public node_t {
    public:
        explicit node_data_t(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::document::document_ptr>&& documents);

        explicit node_data_t(std::pmr::memory_resource* resource,
                             const std::pmr::vector<components::document::document_ptr>& documents);

        const std::pmr::vector<components::document::document_ptr>& documents() const;

    private:
        std::pmr::vector<components::document::document_ptr> documents_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_data_ptr = boost::intrusive_ptr<node_data_t>;

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     std::pmr::vector<components::document::document_ptr>&& documents);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<components::document::document_ptr>& documents);

} // namespace components::logical_plan
