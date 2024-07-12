#include "update_many.hpp"

namespace components::ql {

    update_many_t::~update_many_t() = default;

    update_many_t::update_many_t(const database_name_t& database,
                                 const collection_name_t& collection,
                                 const aggregate::match_t& match,
                                 const storage_parameters& parameters,
                                 const components::document::document_ptr& update,
                                 bool upsert)
        : ql_param_statement_t(statement_type::update_many, database, collection, update->get_allocator())
        , match_(match)
        , update_(update)
        , upsert_(upsert) {
        set_parameters(parameters);
    }

    update_many_t::update_many_t(const database_name_t& database,
                                 const collection_name_t& collection,
                                 std::pmr::memory_resource* resource)
        : ql_param_statement_t(statement_type::update_many, database, collection, resource)
        , update_(document::make_document(resource))
        , upsert_(false) {}

    update_many_t::update_many_t(components::ql::aggregate_statement_raw_ptr condition,
                                 const components::document::document_ptr& update,
                                 bool upsert)
        : ql_param_statement_t(statement_type::update_many,
                               condition->database_,
                               condition->collection_,
                               update->get_allocator())
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
