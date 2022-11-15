#include "aggregate.hpp"
#include <magic_enum.hpp>

namespace components::ql {

    aggregate_statement::aggregate_statement(database_name_t database, collection_name_t collection)
        : ql_statement_t(statement_type::aggregate, std::move(database), std::move(collection)) {}

    void aggregate_statement::append(aggregate::operator_type type, aggregate::operator_storage_t storage) {
        aggregate_operator_.append(type, std::move(storage));
    }

    void aggregate_statement::reserve(std::size_t size) {
        aggregate_operator_.reserve(size);
    }

    auto aggregate_statement::next_id() -> core::parameter_id_t {
        auto tmp = counter_;
        counter_ += 1;
        return core::parameter_id_t(tmp);
    }

    aggregate::operator_type get_aggregate_type(const std::string& key) {
        auto type = magic_enum::enum_cast<aggregate::operator_type>(key);

        if (type.has_value()) {
            return type.value();
        }

        return aggregate::operator_type::invalid;
    }

    template<>
    void aggregate_statement::add_parameter(core::parameter_id_t id, expr_value_t value) {
        values_.emplace(id, value);
    }

    template<>
    void aggregate_statement::add_parameter(core::parameter_id_t id, const ::document::impl::value_t* value) {
        values_.emplace(id, expr_value_t(value));
    }

    template<>
    void aggregate_statement::add_parameter(core::parameter_id_t id, const std::string& value) {
        values_.emplace(id, expr_value_t(::document::impl::new_value(::document::slice_t(value)).detach()));
    }

    auto aggregate_statement::parameter(core::parameter_id_t id) const -> const expr_value_t& {
        auto it = values_.find(id);
        if (it != values_.end()) {
            return it->second;
        }
        static expr_value_t null_(nullptr);
        return null_;
    }

    auto aggregate_statement::count_operators() const -> std::size_t {
        return aggregate_operator_.size();
    }

    auto aggregate_statement::type_operator(std::size_t index) const -> aggregate::operator_type {
        return aggregate_operator_.type(index);
    }

} // namespace components::ql
