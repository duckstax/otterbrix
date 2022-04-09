#include "insert_one.hpp"

insert_one_t::~insert_one_t() = default;

insert_one_t::insert_one_t(const std::string& database, const std::string& collection, components::document::document_ptr document)
    : statement_t(statement_type::insert_one, database, collection)
    , document_(std::move(document)){}
