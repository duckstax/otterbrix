#include "delete_many.hpp"

delete_many_t::~delete_many_t() = default;

delete_many_t::delete_many_t(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition)
    : statement_t(statement_type::delete_many, database, collection)
    , condition_(std::move(condition)) {}
