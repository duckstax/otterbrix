#include "aggregate.hpp"

#include <magic_enum.hpp>

namespace components::ql {

    aggregate_statement::aggregate_statement(database_name_t database, collection_name_t collection)
        : ql_statement_t(statement_type::aggregate, std::move(database), std::move(collection)) {}

    void aggregate_statement::append(aggregate::operator_type type, aggregate::operator_storage_t storage) {
        aggregate_operator_.append(type, std::move(storage));
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

} // namespace components::ql
