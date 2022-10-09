#include "aggregate.hpp"

namespace components::ql {

    aggregate_statement::aggregate_statement(database_name_t database, collection_name_t collection)
        : ql_statement_t(statement_type::aggregate, std::move(database), std::move(collection)) {}
} // namespace components::ql
