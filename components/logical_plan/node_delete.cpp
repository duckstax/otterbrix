#include "node_delete.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include <sstream>

namespace components::logical_plan {

    node_delete_t::node_delete_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const node_match_ptr& match,
                                 const node_limit_ptr& limit)
        : node_t(resource, node_type::delete_t, collection) {
        append_child(match);
        append_child(limit);
    }

    hash_t node_delete_t::hash_impl() const { return 0; }

    std::string node_delete_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$delete: {";
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

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match) {
        return {
            new node_delete_t{resource, collection, match, make_node_limit(resource, collection, limit_t::unlimit())}};
    }

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match) {
        return {new node_delete_t{resource,
                                  collection,
                                  match,
                                  make_node_limit(resource, collection, limit_t::limit_one())}};
    }
    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit) {
        return {new node_delete_t{resource, collection, match, limit}};
    }

    node_delete_ptr to_node_delete(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 4) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto match = to_node_match(msg_object.via.array.ptr[2], resource);
        auto limit = to_node_limit(msg_object.via.array.ptr[3], resource);
        return make_node_delete(resource, {database, collection}, match, limit);
    }

} // namespace components::logical_plan
