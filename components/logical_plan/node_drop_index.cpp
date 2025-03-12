#include "node_drop_index.hpp"
#include <sstream>

namespace components::logical_plan {

    node_drop_index_t::node_drop_index_t(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const std::string& name)
        : node_t(resource, node_type::drop_index_t, collection)
        , name_(name) {}

    const std::string& node_drop_index_t::name() const noexcept { return name_; }

    hash_t node_drop_index_t::hash_impl() const { return 0; }

    std::string node_drop_index_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_index: " << database_name() << "." << collection_name() << " name:" << name();
        return stream.str();
    }

    node_drop_index_ptr make_node_drop_index(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             const std::string& name) {
        return {new node_drop_index_t{resource, collection, name}};
    }
    node_drop_index_ptr to_node_drop_index(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 53) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto name = msg_object.via.array.ptr[2].as<std::string>();
        return make_node_drop_index(resource, {database, collection}, name);
    }

} // namespace components::logical_plan
