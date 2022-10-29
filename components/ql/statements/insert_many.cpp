#include "insert_many.hpp"

namespace components::ql {
    insert_many_t::~insert_many_t() = default;

    insert_many_t::insert_many_t(const std::string& database, const std::string& collection, const std::pmr::vector<components::document::document_ptr>& documents)
        : ql_statement_t(statement_type::insert_many, database, collection)
        , documents_(documents) {}
}