#include "node_insert.hpp"
#include <sstream>

namespace components::logical_plan {

    node_insert_t::node_insert_t(std::pmr::memory_resource *resource,
                                 const collection_full_name_t& collection,
                                 std::pmr::vector<components::document::document_ptr>&& documents)
        : node_t(resource, node_type::insert_t, collection)
        , documents_(std::move(documents)) {
    }

    const std::pmr::vector<document::document_ptr> &node_insert_t::documents() const {
        return documents_;
    }

    hash_t node_insert_t::hash_impl() const {
        return 0;
    }

    std::string node_insert_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$insert: {";
        //todo: all documents
        stream << "$documents: " << documents_.size();
        stream << "}";
        return stream.str();
    }

    node_ptr make_node_insert(std::pmr::memory_resource *resource,
                              ql::insert_many_t *insert) {
        auto node = new node_insert_t{resource, {insert->database_, insert->collection_}, std::move(insert->documents_)};
        return node;
    }

    node_ptr make_node_insert(std::pmr::memory_resource *resource,
                              ql::insert_one_t *insert) {
        std::pmr::vector<components::document::document_ptr> documents = {insert->document_};
        auto node = new node_insert_t{resource, {insert->database_, insert->collection_}, std::move(documents)};
        return node;
    }

}
