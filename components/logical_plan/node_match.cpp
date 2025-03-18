#include "node_match.hpp"
#include <components/expressions/msgpack.hpp>
#include <sstream>

namespace components::logical_plan {

    node_match_t::node_match_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::match_t, collection) {}

    hash_t node_match_t::hash_impl() const { return 0; }

    std::string node_match_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$match: {";
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

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const expressions::expression_ptr& match) {
        node_match_ptr node = new node_match_t{resource, collection};
        if (match) {
            node->append_expression(match);
        }
        return node;
    }

    node_match_ptr to_node_match(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 3) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto expr = expressions::to_expression(msg_object.via.array.ptr[2], resource);
        return make_node_match(resource, {database, collection}, expr);
    }

} // namespace components::logical_plan