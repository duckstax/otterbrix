#include "node_update.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_update_t::node_update_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection_to,
                                 const collection_full_name_t& collection_from,
                                 const node_match_ptr& match,
                                 const node_limit_ptr& limit,
                                 const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                 bool upsert)
        : node_t(resource, node_type::update_t, collection_to)
        , collection_from_(collection_from)
        , update_expressions_(updates)
        , upsert_(upsert) {
        append_child(match);
        append_child(limit);
    }

    const std::pmr::vector<expressions::update_expr_ptr>& node_update_t::updates() const { return update_expressions_; }

    bool node_update_t::upsert() const { return upsert_; }

    const collection_full_name_t& node_update_t::collection_from() const { return collection_from_; }

    node_ptr node_update_t::deserialize(serializer::base_deserializer_t* deserializer) {
        auto collection_to = deserializer->deserialize_collection(1);
        auto collection_from = deserializer->deserialize_collection(2);
        auto children = deserializer->deserialize_nodes(3);
        auto updates = deserializer->deserialize_update_expressions(4);
        auto upsert = deserializer->deserialize_bool(5);
        return make_node_update(deserializer->resource(),
                                collection_to,
                                collection_from,
                                reinterpret_cast<const node_match_ptr&>(children.at(0)),
                                reinterpret_cast<const node_limit_ptr&>(children.at(1)),
                                updates,
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
        serializer->start_array(6);
        serializer->append("type", serializer::serialization_type::logical_node_update);
        serializer->append("collection_to", collection_);
        serializer->append("collection_from", collection_from_);
        serializer->append("child nodes", children_);
        serializer->append("updates", update_expressions_);
        serializer->append("upsert", upsert_);
        serializer->end_array();
    }

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert) {
        return {new node_update_t{resource,
                                  collection,
                                  {},
                                  match,
                                  make_node_limit(resource, collection, limit_t::unlimit()),
                                  updates,
                                  upsert}};
    }

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection_to,
                                          const collection_full_name_t& collection_from,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert) {
        return {new node_update_t{resource,
                                  collection_to,
                                  collection_from,
                                  match,
                                  make_node_limit(resource, collection_to, limit_t::unlimit()),
                                  updates,
                                  upsert}};
    }

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert) {
        return {new node_update_t{resource,
                                  collection,
                                  {},
                                  match,
                                  make_node_limit(resource, collection, limit_t::limit_one()),
                                  updates,
                                  upsert}};
    }

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection_to,
                                         const collection_full_name_t& collection_from,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert) {
        return {new node_update_t{resource,
                                  collection_to,
                                  collection_from,
                                  match,
                                  make_node_limit(resource, collection_to, limit_t::limit_one()),
                                  updates,
                                  upsert}};
    }

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert) {
        return {new node_update_t{resource, collection, {}, match, limit, updates, upsert}};
    }

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection_to,
                                     const collection_full_name_t& collection_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert) {
        return {new node_update_t{resource, collection_to, collection_from, match, limit, updates, upsert}};
    }

} // namespace components::logical_plan
