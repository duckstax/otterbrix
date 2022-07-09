#pragma once

#include <string>
#include <vector>
#include <variant>

#include <components/document/document.hpp>

using database_name_t = std::string;
using collection_name_t = std::string;

enum class statement_type : char {
    unused = 0x00, // unused
    create_database,
    drop_database,
    create_collection,
    drop_collection,
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
    ql_statement_t(statement_type type,  const std::string& database, const std::string& collection)
        : type_(type)
        , database_(database)
        , collection_(collection) {}

    ql_statement_t() :type_(statement_type::unused){}

    virtual ~ql_statement_t()= default;

    ///    std::vector<Expr*>* hints;

    statement_type type() const {
        return type_;
    }

    statement_type type_;
    database_name_t database_;
    collection_name_t collection_;
};

enum class  join_t { inner, full, left, right, cross, natural };

struct collection_ref_t {

};

struct join_definition_t {
    join_definition_t();
    virtual ~join_definition_t();

    collection_ref_t* left;
    collection_ref_t* right;
    /// Expr* condition_t;

    join_t type;
};