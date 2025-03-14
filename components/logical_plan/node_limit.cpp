#include "node_limit.hpp"
#include <sstream>

namespace components::logical_plan {

    limit_t::limit_t(int data)
        : limit_(data) {}

    limit_t limit_t::unlimit() { return limit_t(); }

    limit_t limit_t::limit_one() { return limit_t(1); }

    int limit_t::limit() const { return limit_; }

    bool limit_t::check(int count) const { return limit_ == unlimit_ || limit_ > count; }

    node_limit_t::node_limit_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               const limit_t& limit)
        : node_t(resource, node_type::limit_t, collection)
        , limit_(limit) {}

    const limit_t& node_limit_t::limit() const { return limit_; }

    hash_t node_limit_t::hash_impl() const { return 0; }

    std::string node_limit_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$limit: " << limit_.limit();
        return stream.str();
    }

    node_limit_ptr make_node_limit(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const limit_t& limit) {
        return {new node_limit_t{resource, collection, limit}};
    }

    node_limit_ptr to_node_limit(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 3) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto limit = msg_object.via.array.ptr[2].as<int>();
        return make_node_limit(resource, {database, collection}, limit_t(limit));
    }
} // namespace components::logical_plan
