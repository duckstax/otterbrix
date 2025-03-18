#include "node_sort.hpp"
#include <components/expressions/msgpack.hpp>
#include <sstream>

namespace components::logical_plan {

    node_sort_t::node_sort_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::sort_t, collection) {}

    hash_t node_sort_t::hash_impl() const { return 0; }

    std::string node_sort_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$sort: {";
        bool is_first = true;
        for (const auto& expr : expressions_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }
    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::vector<expressions::expression_ptr>& expressions) {
        auto node = new node_sort_t{resource, collection};
        node->append_expressions(expressions);
        return node;
    }

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::pmr::vector<expressions::expression_ptr>& expressions) {
        auto node = new node_sort_t{resource, collection};
        node->append_expressions(expressions);
        return node;
    }
    node_sort_ptr to_node_sort(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 3) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto expr = expressions::to_expressions(msg_object.via.array.ptr[2], resource);
        return make_node_sort(resource, {database, collection}, expr);
    }

} // namespace components::logical_plan