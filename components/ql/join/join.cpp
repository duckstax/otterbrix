#include "join.hpp"

namespace components::ql {

    join_t::join_t(std::pmr::memory_resource* resource)
        : ql_param_statement_t(statement_type::join, {}, {}, resource) {}

    join_t::join_t(database_name_t database, collection_name_t collection, std::pmr::memory_resource* resource)
        : ql_param_statement_t(statement_type::join, database, collection, resource) {}

    join_t::join_t(database_name_t database,
                   collection_name_t collection,
                   join_type join,
                   std::pmr::memory_resource* resource)
        : ql_param_statement_t(statement_type::join, database, collection, resource)
        , join(join) {}

    std::string join_t::to_string() const {
        std::stringstream s;
        s << "ql: " << left->database_ << "." << left->collection_ << " and " << right->database_ << "."
          << right->collection_;
        return s.str();
    }

    join_ptr make_join(join_type join, std::pmr::memory_resource* resource) {
        return new join_t(database_name_t(), collection_name_t(), join, resource);
    }

} // namespace components::ql
