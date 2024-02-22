#include "join.hpp"

namespace components::ql {

    join_t::join_t()
        : ql_param_statement_t(statement_type::join, {}, {}) {}

    join_t::join_t(database_name_t database, collection_name_t collection)
        : ql_param_statement_t(statement_type::join, database, collection) {}

    join_t::join_t(database_name_t database, collection_name_t collection, join_type join)
        : ql_param_statement_t(statement_type::join, database, collection)
        , join(join) {}

    std::string join_t::to_string() const {
        std::stringstream s;
        s << "ql: " << left->database_ << "." << left->collection_ << " and " << right->database_ << "."
          << right->collection_;
        return s.str();
    }

    join_ptr make_join(join_type join) { return new join_t(database_name_t(), collection_name_t(), join); }

} // namespace components::ql
