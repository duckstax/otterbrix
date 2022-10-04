#pragma once

#include <string>

using database_name_t = std::string;
using collection_name_t = std::string;

namespace components::ql {

    enum class statement_type : char {
        unused = 0x00, // unused
        create_database,
        drop_database,
        create_collection,
        drop_collection,
        find,
        find_one,
        insert_one,
        insert_many,
        delete_one,
        delete_many,
        update_one,
        update_many,
        create_index
    };

    // Base struct for every QL statement
    struct ql_statement_t {
        ql_statement_t(statement_type type, database_name_t database, collection_name_t collection)
            : type_(type)
            , database_(std::move(database))
            , collection_(std::move(collection)) {}

        ql_statement_t() = default;
        virtual ~ql_statement_t() = default;

        statement_type type() const {
            return type_;
        }

        statement_type type_ {statement_type::unused};
        database_name_t database_;
        collection_name_t collection_;
    };

} // namespace components::ql