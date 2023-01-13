#include "delete_one.hpp"

namespace components::ql {

    delete_one_t::~delete_one_t() = default;

    delete_one_t::delete_one_t(const database_name_t &database, const collection_name_t &collection, aggregate::match_t match)
        : ql_statement_t(statement_type::delete_one, database, collection)
        , match_(match) {
    }

    delete_one_t::delete_one_t(components::ql::aggregate_statement_raw_ptr condition)
        : ql_statement_t(statement_type::delete_one, condition->database_, condition->collection_) {
        if (condition->count_operators() > 0) {
            match_ = condition->get_operator<components::ql::aggregate::match_t>(0);
        } else {
            match_.query = nullptr;
        }
    }

} // namespace components::ql
