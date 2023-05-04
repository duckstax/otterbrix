#include "update_one.hpp"

namespace components::ql {

    update_one_t::~update_one_t() = default;

    update_one_t::update_one_t(const database_name_t &database,
                               const collection_name_t &collection,
                               const aggregate::match_t& match,
                               const storage_parameters& parameters,
                               const components::document::document_ptr& update,
                               bool upsert)
        : ql_param_statement_t(statement_type::update_one, database, collection)
        , match_(match)
        , update_(update)
        , upsert_(upsert) {
        set_parameters(parameters);
    }

    update_one_t::update_one_t(components::ql::aggregate_statement_raw_ptr condition,
                               const components::document::document_ptr& update,
                               bool upsert)
        : ql_param_statement_t(statement_type::update_one, condition->database_, condition->collection_)
        , update_(update)
        , upsert_(upsert) {
        if (condition->count_operators() > 0) {
            match_ = condition->get_operator<components::ql::aggregate::match_t>(0);
        } else {
            match_.query = nullptr;
        }
        set_parameters(condition->parameters());
    }

} // namespace components::ql
