#include "aggregate_expression.hpp"

#include <boost/container_hash/hash.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <sstream>

namespace components::expressions {

    template<class OStream>
    OStream& operator<<(OStream& stream, const aggregate_expression_t* expr) {
        if (expr->params().empty()) {
            stream << expr->key();
        } else {
            if (!expr->key().is_null()) {
                stream << expr->key() << ": ";
            }
            stream << "{$" << magic_enum::enum_name(expr->type()) << ": ";
            if (expr->params().size() > 1) {
                stream << "[";
                bool is_first = true;
                for (const auto& param : expr->params()) {
                    if (is_first) {
                        is_first = false;
                    } else {
                        stream << ", ";
                    }
                    stream << param;
                }
                stream << "]";
            } else {
                stream << expr->params().at(0);
            }
            stream << "}";
        }
        return stream;
    }

    aggregate_expression_t::aggregate_expression_t(std::pmr::memory_resource* resource,
                                                   aggregate_type type,
                                                   const key_t& key)
        : expression_i(expression_group::aggregate)
        , type_(type)
        , key_(key)
        , params_(resource) {}

    aggregate_type aggregate_expression_t::type() const { return type_; }

    const key_t& aggregate_expression_t::key() const { return key_; }

    const std::pmr::vector<param_storage>& aggregate_expression_t::params() const { return params_; }

    void aggregate_expression_t::append_param(const param_storage& param) { params_.push_back(param); }

    expression_ptr aggregate_expression_t::deserialize(serializer::base_deserializer_t* deserializer) {
        auto type = deserializer->deserialize_aggregate_type(1);
        auto key = deserializer->deserialize_key(2);
        auto params = deserializer->deserialize_param_storages(3);
        auto res = make_aggregate_expression(deserializer->resource(), type, key);
        for (const auto& param : params) {
            res->append_param(param);
        }
        return res;
    }

    hash_t aggregate_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, type_);
        boost::hash_combine(hash_, key_.hash());
        for (const auto& param : params_) {
            auto param_hash = std::visit(
                [](const auto& value) {
                    using param_type = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<param_type, core::parameter_id_t>) {
                        return std::hash<uint64_t>()(value);
                    } else if constexpr (std::is_same_v<param_type, key_t>) {
                        return value.hash();
                    } else if constexpr (std::is_same_v<param_type, expression_ptr>) {
                        return value->hash();
                    }
                },
                param);
            boost::hash_combine(hash_, param_hash);
        }
        return hash_;
    }

    std::string aggregate_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << this;
        return stream.str();
    }

    bool aggregate_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const aggregate_expression_t*>(rhs);
        return type_ == other->type_ && key_ == other->key_ && params_.size() == other->params_.size() &&
               std::equal(params_.begin(), params_.end(), other->params_.begin());
    }

    void aggregate_expression_t::serialize_impl(serializer::base_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append("type", serializer::serialization_type::expression_aggregate);
        serializer->append("aggregate type", type_);
        serializer->append("key", key_);
        serializer->append("parameters", params_);
        serializer->end_array();
    }

    aggregate_expression_ptr
    make_aggregate_expression(std::pmr::memory_resource* resource, aggregate_type type, const key_t& key) {
        return new aggregate_expression_t(resource, type, key);
    }

    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource, aggregate_type type) {
        return make_aggregate_expression(resource, type, key_t());
    }

    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       aggregate_type type,
                                                       const key_t& key,
                                                       const key_t& field) {
        auto expr = make_aggregate_expression(resource, type, key);
        expr->append_param(field);
        return expr;
    }

    aggregate_type get_aggregate_type(const std::string& key) {
        auto type = magic_enum::enum_cast<aggregate_type>(key);
        if (type.has_value()) {
            return type.value();
        }
        return aggregate_type::invalid;
    }

    bool is_aggregate_type(const std::string& key) { return magic_enum::enum_cast<aggregate_type>(key).has_value(); }

} // namespace components::expressions