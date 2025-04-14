#include "node_update.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <components/document/msgpack/msgpack_encoder.hpp>
#include <sstream>

namespace components::logical_plan {

    node_update_t::node_update_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const node_match_ptr& match,
                                 const node_limit_ptr& limit,
                                 const components::document::document_ptr& update,
                                 bool upsert)
        : node_t(resource, node_type::update_t, collection)
        , update_(update)
        , upsert_(upsert) {
        append_child(match);
        append_child(limit);
    }

    const document::document_ptr& node_update_t::update() const { return update_; }

    bool node_update_t::upsert() const { return upsert_; }

    node_ptr node_update_t::deserialize(serializer::base_deserializer_t* deserializer) {
        auto collection = deserializer->deserialize_collection(1);
        auto children = deserializer->deserialize_nodes(2);
        auto update = deserializer->deserialize_document(3);
        auto upsert = deserializer->deserialize_bool(4);
        return make_node_update(deserializer->resource(),
                                collection,
                                reinterpret_cast<const node_match_ptr&>(children.at(0)),
                                reinterpret_cast<const node_limit_ptr&>(children.at(1)),
                                update,
                                upsert);
    }

    hash_t node_update_t::hash_impl() const { return 0; }

    std::string node_update_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$update: {";
        // TODO: sort fields in to_json() method for consistent results
        // phisical field order in document is random, and to_json is too unreliable for testing with multiple fields
        // stream << update_->to_json() << ", ";
        stream << "$upsert: " << upsert_ << ", ";
        bool is_first = true;
        for (auto child : children()) {
            if (!is_first) {
                stream << ", ";
            } else {
                is_first = false;
            }
            stream << child;
        }
        stream << "}";
        return stream.str();
    }

    void node_update_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_array(5);
        serializer->append("type", serializer::serialization_type::logical_node_update);
        serializer->append("collection", collection_);
        serializer->append("child nodes", children_);
        serializer->append("update", update_);
        serializer->append("upsert", upsert_);
        serializer->end_array();
    }

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match,
                                          const components::document::document_ptr& update,
                                          bool upsert) {
        return {new node_update_t{resource,
                                  collection,
                                  match,
                                  make_node_limit(resource, collection, limit_t::unlimit()),
                                  update,
                                  upsert}};
    }

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match,
                                         const components::document::document_ptr& update,
                                         bool upsert) {
        return {new node_update_t{resource,
                                  collection,
                                  match,
                                  make_node_limit(resource, collection, limit_t::limit_one()),
                                  update,
                                  upsert}};
    }
    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const components::document::document_ptr& update,
                                     bool upsert) {
        return {new node_update_t{resource, collection, match, limit, update, upsert}};
    }

} // namespace components::logical_plan
