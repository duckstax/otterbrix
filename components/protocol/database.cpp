#include "database.hpp"

namespace components::protocol {

    statement_database_t::statement_database_t(statement_type type, const database_name_t& database)
        : statement_t(type, database, std::string()) {
    }

    create_database_t::create_database_t(const database_name_t& database)
        : statement_database_t(statement_type::create_database, database) {
    }

    drop_database_t::drop_database_t(const database_name_t& database)
        : statement_database_t(statement_type::drop_database, database) {
    }

} // namespace components::protocol
