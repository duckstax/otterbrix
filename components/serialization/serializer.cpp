#include "serializer.hpp"

#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/expressions/key.hpp>
#include <logical_plan/node_limit.hpp>

namespace components::serializer {

    base_serializer_t::base_serializer_t(std::pmr::memory_resource* resource)
        : result_(std::pmr::string(resource)) {}

    // TODO: use memory_resource to initialize root_obj_
    json_serializer_t::json_serializer_t(std::pmr::memory_resource* resource)
        : base_serializer_t(resource) {
        working_tree_.emplace(&root_obj_);
    }

    std::pmr::string json_serializer_t::result() const { return result_.str(); }

    void json_serializer_t::start_map(std::string_view key, size_t size) {
        assert(!working_tree_.empty());
        auto it = (*working_tree_.top()).insert_or_assign(key, boost::json::object());
        assert(it.second);
        working_tree_.emplace(&it.first->value().as_object());
    }

    void json_serializer_t::end_map() {
        if (working_tree_.size() == 1) {
            result_ << root_obj_;
        } else {
            working_tree_.pop();
        }
    }

    void json_serializer_t::append(std::string_view key, bool val) { (*working_tree_.top())[key] = val; }

    void json_serializer_t::append(std::string_view key, logical_plan::node_type type) {
        (*working_tree_.top())[key] = static_cast<uint8_t>(type);
    }

    void json_serializer_t::append(std::string_view key, logical_plan::index_type type) {
        (*working_tree_.top())[key] = static_cast<uint8_t>(type);
    }

    void json_serializer_t::append(std::string_view key, logical_plan::join_type type) {
        (*working_tree_.top())[key] = static_cast<uint8_t>(type);
    }

    void json_serializer_t::append(std::string_view key, expressions::aggregate_type type) {
        (*working_tree_.top())[key] = static_cast<uint8_t>(type);
    }

    void json_serializer_t::append(std::string_view key, expressions::compare_type type) {
        (*working_tree_.top())[key] = static_cast<uint8_t>(type);
    }

    void json_serializer_t::append(std::string_view key, expressions::scalar_type type) {
        (*working_tree_.top())[key] = static_cast<uint8_t>(type);
    }

    void json_serializer_t::append(std::string_view key, expressions::sort_order order) {
        (*working_tree_.top())[key] = static_cast<uint8_t>(order);
    }

    void json_serializer_t::append(std::string_view key, logical_plan::limit_t limit) {
        (*working_tree_.top())[key] = limit.limit();
    }

    void json_serializer_t::append(std::string_view key, const std::pmr::vector<logical_plan::node_ptr>& nodes) {
        start_map(key, nodes.size());
        for (const auto& n : nodes) {
            n->serialize(this);
        }
        end_map();
    }

    void json_serializer_t::append(std::string_view key, const std::pmr::vector<document::document_ptr>& documents) {
        auto it = (*working_tree_.top()).insert_or_assign(key, boost::json::array());
        assert(it.second);
        for (const auto& doc : documents) {
            it.first->value().as_array().emplace_back(doc->to_json());
        }
    }

    void json_serializer_t::append(std::string_view key, const std::pmr::vector<expressions::key_t>& keys) {
        auto it = (*working_tree_.top()).insert_or_assign(key, boost::json::array());
        assert(it.second);
        for (const auto& k : keys) {
            it.first->value().as_array().emplace_back(k.as_string());
        }
    }

    void json_serializer_t::append(std::string_view key, const std::pmr::vector<core::parameter_id_t>& params) {
        auto it = (*working_tree_.top()).insert_or_assign(key, boost::json::array());
        assert(it.second);
        for (const auto& id : params) {
            it.first->value().as_array().emplace_back(id);
        }
    }

    void json_serializer_t::append(std::string_view key,
                                   const std::pmr::vector<expressions::expression_ptr>& expressions) {
        start_map(key, expressions.size());
        for (const auto& expr : expressions) {
            expr->serialize(this);
        }
        end_map();
    }

    void json_serializer_t::append(std::string_view key,
                                   const std::pmr::vector<expressions::compare_expression_ptr>& expressions) {
        start_map(key, expressions.size());
        for (const auto& expr : expressions) {
            expr->serialize(this);
        }
        end_map();
    }

    void json_serializer_t::append(std::string_view key, const collection_full_name_t& collection) {
        auto it = (*working_tree_.top()).insert_or_assign(key, boost::json::array());
        assert(it.second);
        it.first->value().as_array().emplace_back(collection.database);
        it.first->value().as_array().emplace_back(collection.collection);
    }

    void json_serializer_t::append(std::string_view key, const std::string& str) { (*working_tree_.top())[key] = str; }

    void json_serializer_t::append(std::string_view key, const document::document_ptr& doc) {
        (*working_tree_.top())[key] = doc->to_json();
    }

    void json_serializer_t::append(std::string_view key, const document::value_t& val) {
        switch (val.physical_type()) {
            case types::physical_type::BOOL:
                (*working_tree_.top())[key] = val.as_bool();
                break;
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                (*working_tree_.top())[key] = val.as_unsigned();
                break;
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                (*working_tree_.top())[key] = val.as_int();
                break;
            // case types::physical_type::UINT128:
            // case types::physical_type::INT128:
            //     (*working_tree_.top())[key] = val.as_int128();
            //     break;
            case types::physical_type::FLOAT:
                (*working_tree_.top())[key] = val.as_float();
                break;
            case types::physical_type::DOUBLE:
                (*working_tree_.top())[key] = val.as_double();
                break;
            case types::physical_type::STRING:
                (*working_tree_.top())[key] = val.as_string();
                break;
            case types::physical_type::NA:
                (*working_tree_.top())[key] = nullptr;
                break;
            default:
                assert(false && "incorrect type");
        }
    }

    void json_serializer_t::append(std::string_view key, const expressions::key_t& key_val) {
        (*working_tree_.top())[key] = key_val.as_string();
    }

    std::pmr::string msgpack_serializer_t::result() const { return result_.str(); }

    void msgpack_serializer_t::start_map(std::string_view key, size_t size) { packer_.pack_array(size); }

    void msgpack_serializer_t::end_map() {
        // nothing to do here
    }

    void msgpack_serializer_t::append(std::string_view key, bool val) { packer_.pack(val); }

    void msgpack_serializer_t::append(std::string_view key, logical_plan::node_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, logical_plan::index_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, logical_plan::join_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::aggregate_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::compare_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::scalar_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::sort_order order) {
        packer_.pack(static_cast<uint8_t>(order));
    }

    void msgpack_serializer_t::append(std::string_view key, logical_plan::limit_t limit) {
        packer_.pack(limit.limit());
    }

    void msgpack_serializer_t::append(std::string_view key, const std::pmr::vector<logical_plan::node_ptr>& nodes) {
        packer_.pack_array(nodes.size());
        for (const auto& n : nodes) {
            n->serialize(this);
        }
    }

    void msgpack_serializer_t::append(std::string_view key, const std::pmr::vector<document::document_ptr>& documents) {
        packer_.pack_array(documents.size());
        for (const auto& doc : documents) {
            packer_.pack(doc);
        }
    }

    void msgpack_serializer_t::append(std::string_view key, const std::pmr::vector<expressions::key_t>& keys) {
        packer_.pack_array(keys.size());
        for (const auto& k : keys) {
            packer_.pack(k.as_string());
        }
    }

    void msgpack_serializer_t::append(std::string_view key, const std::pmr::vector<core::parameter_id_t>& params) {
        packer_.pack_array(params.size());
        for (const auto& id : params) {
            packer_.pack(id.t);
        }
    }

    void msgpack_serializer_t::append(std::string_view key,
                                      const std::pmr::vector<expressions::expression_ptr>& expressions) {
        start_map(key, expressions.size());
        for (const auto& expr : expressions) {
            expr->serialize(this);
        }
        end_map();
    }

    void msgpack_serializer_t::append(std::string_view key,
                                      const std::pmr::vector<expressions::compare_expression_ptr>& expressions) {
        start_map(key, expressions.size());
        for (const auto& expr : expressions) {
            expr->serialize(this);
        }
        end_map();
    }

    void msgpack_serializer_t::append(std::string_view key, const collection_full_name_t& collection) {
        packer_.pack_array(2);
        packer_.pack(collection.database);
        packer_.pack(collection.collection);
    }

    void msgpack_serializer_t::append(std::string_view key, const std::string& str) { packer_.pack(str); }

    void msgpack_serializer_t::append(std::string_view key, const document::document_ptr& doc) { packer_.pack(doc); }

    void msgpack_serializer_t::append(std::string_view key, const document::value_t& val) { packer_.pack(val); }

    void msgpack_serializer_t::append(std::string_view key, const expressions::key_t& key_val) {
        packer_.pack(key_val.as_string());
    }

} // namespace components::serializer