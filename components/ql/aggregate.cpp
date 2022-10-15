#include "aggregate.hpp"

#include <magic_enum.hpp>

namespace components::ql {

    aggregate_statement::aggregate_statement(database_name_t database, collection_name_t collection)
        : ql_statement_t(statement_type::aggregate, std::move(database), std::move(collection)) {}

    aggregate::operator_type get_aggregate_type(const std::string& key) {
        auto type = magic_enum::enum_cast<aggregate::operator_type>(key);

        if (type.has_value()) {
            return type.value();
        }

        return aggregate::operator_type::invalid;
    }

} // namespace components::ql
