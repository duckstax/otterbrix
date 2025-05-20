#include "update_expression.hpp"

#include <components/logical_plan/param_storage.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

namespace components::expressions {

    update_expr_t::update_expr_t(update_expr_type type)
        : type_(type) {}

    bool update_expr_t::execute(document::document_ptr& to,
                                const document::document_ptr& from,
                                document::impl::base_document* tape,
                                const components::logical_plan::storage_parameters* parameters) {
        if (left_) {
            left_->execute(to, from, tape, parameters);
        }
        if (right_) {
            right_->execute(to, from, tape, parameters);
        }
        return execute_impl(to, from, tape, parameters);
    }

    update_expr_type update_expr_t::type() const noexcept { return type_; }

    update_expr_ptr& update_expr_t::left() { return left_; }

    const update_expr_ptr& update_expr_t::left() const { return left_; }

    update_expr_ptr& update_expr_t::right() { return right_; }

    const update_expr_ptr& update_expr_t::right() const { return right_; }

    document::value_t& update_expr_t::output() { return output_; }

    const document::value_t& update_expr_t::output() const { return output_; }

    update_expr_ptr update_expr_t::deserialize(serializer::base_deserializer_t* deserializer) {
        auto type = deserializer->deserialize_update_expr_type(1);
        switch (type) {
            case update_expr_type::set:
                return update_expr_set_t::deserialize(deserializer);
            case update_expr_type::get_value_doc:
                return update_expr_get_value_t::deserialize(deserializer);
            case update_expr_type::get_value_params:
                return update_expr_get_const_value_t::deserialize(deserializer);
            case update_expr_type::add:
            case update_expr_type::sub:
            case update_expr_type::mult:
            case update_expr_type::div:
            case update_expr_type::mod:
            case update_expr_type::neg:
                return update_expr_calculate_t::deserialize(deserializer);
            default:
                assert(false && "incorrect update_expr_type");
        }
    }

    bool operator==(const update_expr_ptr& lhs, const update_expr_ptr& rhs) {
        if (lhs.get() == rhs.get()) {
            // same address
            return true;
        }
        // XOR
        if ((lhs != nullptr) != (rhs != nullptr)) {
            // only one is nullptr
            return false;
        }
        if (lhs->type() != rhs->type()) {
            return false;
        }

        switch (lhs->type()) {
            case update_expr_type::set:
                return *reinterpret_cast<const update_expr_set_ptr&>(lhs) ==
                       *reinterpret_cast<const update_expr_set_ptr&>(rhs);
            case update_expr_type::get_value_doc:
                return *reinterpret_cast<const update_expr_get_value_ptr&>(lhs) ==
                       *reinterpret_cast<const update_expr_get_value_ptr&>(rhs);
            case update_expr_type::get_value_params:
                return *reinterpret_cast<const update_expr_get_const_value_ptr&>(lhs) ==
                       *reinterpret_cast<const update_expr_get_const_value_ptr&>(rhs);
            case update_expr_type::add:
            case update_expr_type::sub:
            case update_expr_type::mult:
            case update_expr_type::div:
            case update_expr_type::mod:
            case update_expr_type::neg:
                return *reinterpret_cast<const update_expr_calculate_ptr&>(lhs) ==
                       *reinterpret_cast<const update_expr_calculate_ptr&>(rhs);
            default:
                assert(false && "incorrect update_expr_type");
        }
    }

    update_expr_set_t::update_expr_set_t(key_t key)
        : update_expr_t(update_expr_type::set)
        , key_(std::move(key)) {}

    const key_t& update_expr_set_t::key() const noexcept { return key_; }

    bool update_expr_set_t::operator==(const update_expr_set_t& rhs) const {
        return left_ == rhs.left_ && key_ == rhs.key_;
    }

    void update_expr_set_t::serialize(serializer::base_serializer_t* serializer) {
        serializer->start_array(4);
        serializer->append("type", serializer::serialization_type::expression_update);
        serializer->append("expr_type", type_);
        serializer->append("key", key_);
        serializer->append("left", left_);
        serializer->end_array();
    }

    update_expr_ptr update_expr_set_t::deserialize(serializer::base_deserializer_t* deserializer) {
        update_expr_ptr res = new update_expr_set_t(deserializer->deserialize_key(2));
        res->left() = deserializer->deserialize_update_expression(3);
        return res;
    }

    bool update_expr_set_t::execute_impl(document::document_ptr& to,
                                         const document::document_ptr& from,
                                         document::impl::base_document*,
                                         const components::logical_plan::storage_parameters*) {
        if (left_) {
            return to->update(key_.as_string(), left_->output());
        }
        return false;
    }

    update_expr_get_value_t::update_expr_get_value_t(key_t key, side_t side)
        : update_expr_t(update_expr_type::get_value_doc)
        , key_(std::move(key))
        , side_(side) {}

    const key_t& update_expr_get_value_t::key() const noexcept { return key_; }

    update_expr_get_value_t::side_t update_expr_get_value_t::side() const noexcept { return side_; }

    bool update_expr_get_value_t::operator==(const update_expr_get_value_t& rhs) const {
        return left_ == rhs.left_ && key_ == rhs.key_ && side_ == rhs.side_;
    }

    void update_expr_get_value_t::serialize(serializer::base_serializer_t* serializer) {
        serializer->start_array(4);
        serializer->append("type", serializer::serialization_type::expression_update);
        serializer->append("expr_type", type_);
        serializer->append("key", key_);
        serializer->append("side", side_);
        serializer->end_array();
    }

    update_expr_ptr update_expr_get_value_t::deserialize(serializer::base_deserializer_t* deserializer) {
        return {new update_expr_get_value_t(deserializer->deserialize_key(2),
                                            (deserializer->deserialize_update_expr_side(3)))};
    }

    bool update_expr_get_value_t::execute_impl(document::document_ptr& to,
                                               const document::document_ptr& from,
                                               document::impl::base_document* tape,
                                               const components::logical_plan::storage_parameters*) {
        if (side_ == side_t::undefined) {
            if (to->is_exists(key_.as_string())) {
                side_ = side_t::to;
            } else if (from->is_exists(key_.as_string())) {
                side_ = side_t::from;
            } else {
                output_ = document::value_t{tape, nullptr};
            }
        }
        if (side_ == side_t::from) {
            output_ = from->get_value(key_.as_string());
        } else if (side_ == side_t::to) {
            output_ = to->get_value(key_.as_string());
        }
        return false;
    }

    update_expr_get_const_value_t::update_expr_get_const_value_t(core::parameter_id_t id)
        : update_expr_t(update_expr_type::get_value_params)
        , id_(id) {}

    core::parameter_id_t update_expr_get_const_value_t::id() const noexcept { return id_; }

    bool update_expr_get_const_value_t::operator==(const update_expr_get_const_value_t& rhs) const {
        return id_ == rhs.id_;
    }

    void update_expr_get_const_value_t::serialize(serializer::base_serializer_t* serializer) {
        serializer->start_array(3);
        serializer->append("type", serializer::serialization_type::expression_update);
        serializer->append("expr_type", type_);
        serializer->append("id", id_);
        serializer->end_array();
    }

    update_expr_ptr update_expr_get_const_value_t::deserialize(serializer::base_deserializer_t* deserializer) {
        return {new update_expr_get_const_value_t(deserializer->deserialize_param_id(2))};
    }

    bool update_expr_get_const_value_t::execute_impl(document::document_ptr& to,
                                                     const document::document_ptr& from,
                                                     document::impl::base_document*,
                                                     const components::logical_plan::storage_parameters* parameters) {
        output_ = parameters->parameters.at(id_);
        return false;
    }

    update_expr_calculate_t::update_expr_calculate_t(update_expr_type type)
        : update_expr_t(type) {}

    bool update_expr_calculate_t::operator==(const update_expr_calculate_t& rhs) const {
        return left_ == rhs.left_ && right_ == rhs.right_;
    }

    void update_expr_calculate_t::serialize(serializer::base_serializer_t* serializer) {
        serializer->start_array(4);
        serializer->append("type", serializer::serialization_type::expression_update);
        serializer->append("expr_type", type_);
        serializer->append("left", left_);
        serializer->append("right", right_);
        serializer->end_array();
    }

    update_expr_ptr update_expr_calculate_t::deserialize(serializer::base_deserializer_t* deserializer) {
        update_expr_ptr res = new update_expr_calculate_t(deserializer->deserialize_update_expr_type(1));
        res->left() = deserializer->deserialize_update_expression(2);
        res->right() = deserializer->deserialize_update_expression(3);
        return res;
    }

    bool update_expr_calculate_t::execute_impl(document::document_ptr&,
                                               const document::document_ptr&,
                                               document::impl::base_document* tape,
                                               const components::logical_plan::storage_parameters*) {
        switch (type_) {
            case update_expr_type::add:
                output_ = sum(left_->output(), right_->output(), tape);
                break;
            case update_expr_type::sub:
                output_ = subtract(left_->output(), right_->output(), tape);
                break;
            case update_expr_type::mult:
                output_ = mult(left_->output(), right_->output(), tape);
                break;
            case update_expr_type::div:
                output_ = divide(left_->output(), right_->output(), tape);
                break;
            case update_expr_type::mod:
                output_ = modulus(left_->output(), right_->output(), tape);
                break;
            case update_expr_type::neg:
                output_ = negate(left_->output(), tape);
                break;
            default:
                break;
        }
        return false;
    }

} // namespace components::expressions