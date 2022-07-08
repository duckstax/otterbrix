#include "update_many.hpp"

update_many_t::~update_many_t() = default;

update_many_t::update_many_t(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition,
                             components::document::document_ptr update, bool upsert)
    : ql_statement_t(statement_type::update_many, database, collection)
    , condition_(std::move(condition))
    , update_(std::move(update))
    , upsert_(upsert) {}
