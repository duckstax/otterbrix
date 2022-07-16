#pragma once

#include <string>
#include <variant>
#include <vector>

#include <components/document/document.hpp>

namespace components::ql {

    using database_name_t = std::string;
    using collection_name_t = std::string;

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
        ql_statement_t(statement_type type, const std::string& database, const std::string& collection)
            : type_(type)
            , database_(database)
            , collection_(collection) {}

        ql_statement_t() = delete;

        virtual ~ql_statement_t() = default;

        ///    std::vector<Expr*>* hints;

        statement_type type() const {
            return type_;
        }

        statement_type type_;
        database_name_t database_;
        collection_name_t collection_;
    };
} // namespace components::ql