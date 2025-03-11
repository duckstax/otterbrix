#include "node_update.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
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

    node_update_ptr to_node_update(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 6) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto match = to_node_match(msg_object.via.array.ptr[2], resource);
        auto limit = to_node_limit(msg_object.via.array.ptr[3], resource);
        auto update = components::document::to_document(msg_object.via.array.ptr[4], resource);
        auto upsert = msg_object.via.array.ptr[5].as<bool>();
        return make_node_update(resource, {database, collection}, match, limit, update, upsert);
    }

} // namespace components::logical_plan
