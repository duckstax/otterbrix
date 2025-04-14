#include "deserializer.hpp"

#include "logical_plan/node_limit.hpp"

namespace components::serializer {

    base_deserializer_t::base_deserializer_t(const std::pmr::string& input)
        : input_(input) {}

    base_deserializer_t::deserialization_result base_deserializer_t::deserialize(size_t index) {
        advance_array(index);
        auto type = current_type();
        pop_array();
        switch (type) {
            case serialization_type::logical_node_aggregate:
            case serialization_type::logical_node_create_collection:
            case serialization_type::logical_node_create_database:
            case serialization_type::logical_node_create_index:
            case serialization_type::logical_node_data:
            case serialization_type::logical_node_delete:
            case serialization_type::logical_node_drop_collection:
            case serialization_type::logical_node_drop_database:
            case serialization_type::logical_node_drop_index:
            case serialization_type::logical_node_insert:
            case serialization_type::logical_node_join:
            case serialization_type::logical_node_limit:
            case serialization_type::logical_node_match:
            case serialization_type::logical_node_group:
            case serialization_type::logical_node_sort:
            case serialization_type::logical_node_function:
            case serialization_type::logical_node_update:
                return deserialize_logical_node(index);
            case serialization_type::expression_compare:
            case serialization_type::expression_aggregate:
            case serialization_type::expression_scalar:
            case serialization_type::expression_sort:
                return deserialize_expression(index);
            case serialization_type::parameters:
                return deserialize_parameters(index);
            default:
                assert(false);
        }
    }

    std::pmr::vector<expressions::key_t> base_deserializer_t::deserialize_keys(size_t index) {
        std::pmr::vector<expressions::key_t> res(resource());
        advance_array(index);
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_key(i));
        }
        pop_array();
        return res;
    }

    std::pmr::vector<expressions::param_storage> base_deserializer_t::deserialize_param_storages(size_t index) {
        std::pmr::vector<expressions::param_storage> res(resource());
        advance_array(index);
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_param_storage(i));
        }
        pop_array();
        return res;
    }

    std::pmr::vector<document_ptr> base_deserializer_t::deserialize_documents(size_t index) {
        advance_array(index);
        std::pmr::vector<document_ptr> res(resource());
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_document(i));
        }
        pop_array();
        return res;
    }

    std::pmr::vector<core::parameter_id_t> base_deserializer_t::deserialize_param_ids(size_t index) {
        advance_array(index);
        std::pmr::vector<core::parameter_id_t> res(resource());
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_param_id(i));
        }
        pop_array();
        return res;
    }

    std::pmr::vector<logical_plan::node_ptr> base_deserializer_t::deserialize_nodes(size_t index) {
        advance_array(index);
        std::pmr::vector<logical_plan::node_ptr> res(resource());
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_logical_node(i));
        }
        pop_array();
        return res;
    }

    std::pmr::vector<expressions::expression_ptr> base_deserializer_t::deserialize_expressions(size_t index) {
        advance_array(index);
        std::pmr::vector<logical_plan::expression_ptr> res(resource());
        res.reserve(current_array_size());
        for (size_t i = 0; i < current_array_size(); i++) {
            res.emplace_back(deserialize_expression(i));
        }
        pop_array();
        return res;
    }

    std::pair<core::parameter_id_t, document::value_t>
    base_deserializer_t::deserialize_param_pair(document::impl::base_document* tape, size_t index) {
        advance_array(index);
        std::pair res = {deserialize_param_id(0), deserialize_value(tape, 1)};
        pop_array();
        return res;
    }

    logical_plan::node_ptr base_deserializer_t::deserialize_logical_node(size_t index) {
        advance_array(index);
        auto res = logical_plan::node_t::deserialize(this);
        pop_array();
        return res;
    }

    expressions::expression_ptr base_deserializer_t::deserialize_expression(size_t index) {
        advance_array(index);
        auto res = expressions::expression_i::deserialize(this);
        pop_array();
        return res;
    }

    logical_plan::parameter_node_ptr base_deserializer_t::deserialize_parameters(size_t index) {
        advance_array(index);
        auto res = logical_plan::parameter_node_t::deserialize(this);
        pop_array();
        return res;
    }

    json_deserializer_t::json_deserializer_t(const std::pmr::string& input)
        : base_deserializer_t(input) {
        root_arr_ = parse(input_, root_arr_.storage()).as_array();
        working_tree_.emplace(&root_arr_);
    }

    size_t json_deserializer_t::root_array_size() const { return root_arr_.size(); }

    size_t json_deserializer_t::current_array_size() const { return working_tree_.top()->size(); }

    void json_deserializer_t::advance_array(size_t index) {
        working_tree_.emplace(&working_tree_.top()->at(index).as_array());
    }

    void json_deserializer_t::pop_array() { working_tree_.pop(); }

    serialization_type json_deserializer_t::current_type() {
        return static_cast<serialization_type>(working_tree_.top()->at(0).as_int64());
    }

    bool json_deserializer_t::deserialize_bool(size_t index) { return working_tree_.top()->at(index).as_bool(); }

    uint64_t json_deserializer_t::deserialize_uint64(size_t index) {
        return working_tree_.top()->at(index).as_uint64();
    }

    expressions::aggregate_type json_deserializer_t::deserialize_aggregate_type(size_t index) {
        return static_cast<expressions::aggregate_type>(working_tree_.top()->at(index).as_int64());
    }

    expressions::compare_type json_deserializer_t::deserialize_compare_type(size_t index) {
        return static_cast<expressions::compare_type>(working_tree_.top()->at(index).as_int64());
    }

    expressions::scalar_type json_deserializer_t::deserialize_scalar_type(size_t index) {
        return static_cast<expressions::scalar_type>(working_tree_.top()->at(index).as_int64());
    }

    expressions::sort_order json_deserializer_t::deserialize_sort_order(size_t index) {
        return static_cast<expressions::sort_order>(working_tree_.top()->at(index).as_int64());
    }

    logical_plan::index_type json_deserializer_t::deserialize_index_type(size_t index) {
        return static_cast<logical_plan::index_type>(working_tree_.top()->at(index).as_int64());
    }

    logical_plan::join_type json_deserializer_t::deserialize_join_type(size_t index) {
        return static_cast<logical_plan::join_type>(working_tree_.top()->at(index).as_int64());
    }

    logical_plan::limit_t json_deserializer_t::deserialize_limit(size_t index) {
        return logical_plan::limit_t(working_tree_.top()->at(index).as_int64());
    }
    core::parameter_id_t json_deserializer_t::deserialize_param_id(size_t index) {
        return core::parameter_id_t(working_tree_.top()->at(index).as_int64());
    }

    expressions::key_t json_deserializer_t::deserialize_key(size_t index) {
        if (working_tree_.top()->at(index).is_null()) {
            return {};
        } else {
            return expressions::key_t{working_tree_.top()->at(index).as_string()};
        }
    }

    std::string json_deserializer_t::deserialize_string(size_t index) {
        return working_tree_.top()->at(index).as_string().c_str();
    }
    document::value_t json_deserializer_t::deserialize_value(document::impl::base_document* tape, size_t index) {
        auto new_value = [&](auto value) { return document::value_t{tape, value}; };
        switch (working_tree_.top()->at(index).kind()) {
            case boost::json::kind::bool_:
                return new_value(working_tree_.top()->at(index).as_bool());
            case boost::json::kind::int64:
                return new_value(working_tree_.top()->at(index).as_int64());
            case boost::json::kind::uint64:
                return new_value(working_tree_.top()->at(index).as_uint64());
            case boost::json::kind::double_:
                return new_value(working_tree_.top()->at(index).as_double());
            case boost::json::kind::string:
                return new_value(working_tree_.top()->at(index).as_string());
            default:
                return new_value(nullptr);
        }
    }

    document_ptr json_deserializer_t::deserialize_document(size_t index) {
        return document_t::document_from_json(working_tree_.top()->at(index).as_string().c_str(), resource());
    }

    collection_full_name_t json_deserializer_t::deserialize_collection(size_t index) {
        return {working_tree_.top()->at(index).as_array().at(0).as_string().c_str(),
                working_tree_.top()->at(index).as_array().at(1).as_string().c_str()};
    }
    expressions::param_storage json_deserializer_t::deserialize_param_storage(size_t index) {
        if (working_tree_.top()->at(index).is_uint64()) {
            return deserialize_param_id(index);
        } else if (working_tree_.top()->at(index).is_array()) {
            return deserialize_expression(index);
        } else {
            return deserialize_key(index);
        }
    }

    msgpack_deserializer_t::msgpack_deserializer_t(const std::pmr::string& input)
        : base_deserializer_t(input) {
        msg = msgpack::unpack(input_.data(), input_.size());
        root_arr_ = msg.get().via.array;
        working_tree_.emplace(&root_arr_);
    }

    size_t msgpack_deserializer_t::root_array_size() const { return root_arr_.size; }

    size_t msgpack_deserializer_t::current_array_size() const { return working_tree_.top()->size; }

    void msgpack_deserializer_t::advance_array(size_t index) {
        working_tree_.emplace(&working_tree_.top()->ptr[index].via.array);
    }

    void msgpack_deserializer_t::pop_array() { working_tree_.pop(); }

    serialization_type msgpack_deserializer_t::current_type() {
        return static_cast<serialization_type>(working_tree_.top()->ptr[0].via.u64);
    }

    bool msgpack_deserializer_t::deserialize_bool(size_t index) { return working_tree_.top()->ptr[index].via.boolean; }

    uint64_t msgpack_deserializer_t::deserialize_uint64(size_t index) {
        return working_tree_.top()->ptr[index].via.u64;
    }

    expressions::aggregate_type msgpack_deserializer_t::deserialize_aggregate_type(size_t index) {
        return static_cast<expressions::aggregate_type>(working_tree_.top()->ptr[index].via.u64);
    }

    expressions::compare_type msgpack_deserializer_t::deserialize_compare_type(size_t index) {
        return static_cast<expressions::compare_type>(working_tree_.top()->ptr[index].via.u64);
    }

    expressions::scalar_type msgpack_deserializer_t::deserialize_scalar_type(size_t index) {
        return static_cast<expressions::scalar_type>(working_tree_.top()->ptr[index].via.u64);
    }

    expressions::sort_order msgpack_deserializer_t::deserialize_sort_order(size_t index) {
        return static_cast<expressions::sort_order>(working_tree_.top()->ptr[index].via.i64);
    }

    logical_plan::index_type msgpack_deserializer_t::deserialize_index_type(size_t index) {
        return static_cast<logical_plan::index_type>(working_tree_.top()->ptr[index].via.u64);
    }

    logical_plan::join_type msgpack_deserializer_t::deserialize_join_type(size_t index) {
        return static_cast<logical_plan::join_type>(working_tree_.top()->ptr[index].via.u64);
    }

    logical_plan::limit_t msgpack_deserializer_t::deserialize_limit(size_t index) {
        return static_cast<logical_plan::limit_t>(working_tree_.top()->ptr[index].via.i64);
    }

    core::parameter_id_t msgpack_deserializer_t::deserialize_param_id(size_t index) {
        return static_cast<core::parameter_id_t>(working_tree_.top()->ptr[index].via.u64);
    }

    expressions::key_t msgpack_deserializer_t::deserialize_key(size_t index) {
        if (working_tree_.top()->ptr[index].is_nil()) {
            return {};
        } else {
            return expressions::key_t{working_tree_.top()->ptr[index].via.str.ptr,
                                      working_tree_.top()->ptr[index].via.str.size};
        }
    }

    std::string msgpack_deserializer_t::deserialize_string(size_t index) {
        return {working_tree_.top()->ptr[index].via.str.ptr, working_tree_.top()->ptr[index].via.str.size};
    }

    document::value_t msgpack_deserializer_t::deserialize_value(document::impl::base_document* tape, size_t index) {
        auto obj = working_tree_.top()->ptr[index];
        switch (obj.type) {
            case msgpack::type::NIL:
                return document::value_t(tape, nullptr);
            case msgpack::type::BOOLEAN:
                return document::value_t(tape, obj.via.boolean);
            case msgpack::type::POSITIVE_INTEGER:
                return document::value_t(tape, obj.via.u64);
            case msgpack::type::NEGATIVE_INTEGER:
                return document::value_t(tape, obj.via.i64);
            case msgpack::type::FLOAT32:
            case msgpack::type::FLOAT64:
                return document::value_t(tape, obj.via.f64);
            case msgpack::type::STR:
                return document::value_t(tape, std::string_view(obj.via.str.ptr, obj.via.str.size));
            default:
                return document::value_t();
        }
    }

    document_ptr msgpack_deserializer_t::deserialize_document(size_t index) {
        return document::msgpack_decoder_t::to_document(working_tree_.top()->ptr[index], resource());
    }

    collection_full_name_t msgpack_deserializer_t::deserialize_collection(size_t index) {
        return {{working_tree_.top()->ptr[index].via.array.ptr[0].via.str.ptr,
                 working_tree_.top()->ptr[index].via.array.ptr[0].via.str.size},
                {working_tree_.top()->ptr[index].via.array.ptr[1].via.str.ptr,
                 working_tree_.top()->ptr[index].via.array.ptr[1].via.str.size}};
    }

    expressions::param_storage msgpack_deserializer_t::deserialize_param_storage(size_t index) {
        if (working_tree_.top()->ptr[index].type == msgpack::type::NEGATIVE_INTEGER) {
            return deserialize_param_id(index);
        } else if (working_tree_.top()->ptr[index].type == msgpack::type::ARRAY) {
            return deserialize_expression(index);
        } else {
            return deserialize_key(index);
        }
    }

} // namespace components::serializer