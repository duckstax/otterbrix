#include "delete_one.hpp"

delete_one_t::~delete_one_t() = default;

delete_one_t::delete_one_t(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition)
    : statement_t(statement_type::delete_one, database, collection)
    , condition_(std::move(condition)) {}
