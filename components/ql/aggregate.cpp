#include "aggregate.hpp"
#include <magic_enum.hpp>

namespace components::ql {

    aggregate_statement::aggregate_statement(database_name_t database, collection_name_t collection)
        : ql_param_statement_t(statement_type::aggregate, std::move(database), std::move(collection)) {}

    void aggregate_statement::append(aggregate::operator_type type, aggregate::operator_storage_t storage) {
        aggregate_operator_.append(type, std::move(storage));
    }

    void aggregate_statement::reserve(std::size_t size) {
        aggregate_operator_.reserve(size);
    }

    aggregate::operator_type get_aggregate_type(const std::string& key) {
        auto type = magic_enum::enum_cast<aggregate::operator_type>(key);

        if (type.has_value()) {
            return type.value();
        }

        return aggregate::operator_type::invalid;
    }

    auto aggregate_statement::count_operators() const -> std::size_t {
        return aggregate_operator_.size();
    }

    auto aggregate_statement::type_operator(std::size_t index) const -> aggregate::operator_type {
        return aggregate_operator_.type(index);
    }


    aggregate_statement_ptr make_aggregate_statement(const database_name_t &database, const collection_name_t &collection) {
        return std::make_unique<aggregate_statement>(database, collection);
    }

} // namespace components::ql
