#include "node_data.hpp"

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_data_t::node_data_t(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::document::document_ptr>&& documents)
        : node_t(resource, node_type::data_t, {})
        , documents_(std::move(documents)) {}

    node_data_t::node_data_t(std::pmr::memory_resource* resource,
                             const std::pmr::vector<components::document::document_ptr>& documents)
        : node_t(resource, node_type::data_t, {})
        , documents_(documents) {}

    const std::pmr::vector<document::document_ptr>& node_data_t::documents() const { return documents_; }

    hash_t node_data_t::hash_impl() const { return 0; }

    std::string node_data_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$raw_data: {";
        //todo: all documents
        stream << "$documents: " << documents_.size();
        stream << "}";
        return stream.str();
    }

    void node_data_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append("type", std::string("node_data_t"));
        serializer->append("documents", documents_);
        serializer->end_array();
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     std::pmr::vector<components::document::document_ptr>&& documents) {
        return {new node_data_t{resource, std::move(documents)}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<components::document::document_ptr>& documents) {
        return {new node_data_t{resource, documents}};
    }

} // namespace components::logical_plan
