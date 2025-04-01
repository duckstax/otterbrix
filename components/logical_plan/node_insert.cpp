#include "node_insert.hpp"

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_insert_t::node_insert_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 std::pmr::vector<components::document::document_ptr>&& documents)
        : node_t(resource, node_type::insert_t, collection)
        , documents_(std::move(documents)) {}

    node_insert_t::node_insert_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::pmr::vector<components::document::document_ptr>& documents)
        : node_t(resource, node_type::insert_t, collection)
        , documents_(documents) {}

    const std::pmr::vector<document::document_ptr>& node_insert_t::documents() const { return documents_; }

    hash_t node_insert_t::hash_impl() const { return 0; }

    std::string node_insert_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$insert: {";
        //todo: all documents
        stream << "$documents: " << documents_.size();
        stream << "}";
        return stream.str();
    }

    void node_insert_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_map(logical_plan::to_string(type_), 2);
        serializer->append("collection", collection_);
        serializer->append("documents", documents_);
        serializer->end_map();
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const std::pmr::vector<components::document::document_ptr>& documents) {
        return {new node_insert_t{resource, collection, documents}};
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     std::pmr::vector<components::document::document_ptr>&& documents) {
        return {new node_insert_t{resource, collection, std::move(documents)}};
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     document_ptr document) {
        std::pmr::vector<components::document::document_ptr> documents = {std::move(document)};
        return {new node_insert_t{resource, collection, std::move(documents)}};
    }

} // namespace components::logical_plan
