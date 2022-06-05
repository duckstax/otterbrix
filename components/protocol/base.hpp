#pragma once

#include <string>
#include <vector>
#include <variant>

#include <components/document/document.hpp>

using database_name_t = std::string;
using collection_name_t = std::string;

enum class DataType {
    unknown,
    char_,
    date,
    datetime,
    decimal,
    double_,
    float_,
    int_,
    long_,
    real,
    smallint,
    text,
    time,
    varchar,
};

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
    update_many
};

// Base struct for every SQL statement
struct statement_t {
    statement_t(statement_type type,  const std::string& database, const std::string& collection)
        : type_(type)
        , database_(database)
        , collection_(collection) {}

    statement_t() :type_(statement_type::unused){}

    virtual ~statement_t();

    ///    std::vector<Expr*>* hints;

    statement_type type() const {
        return type_;
    }

    statement_type type_;
    database_name_t database_;
    collection_name_t collection_;
};

enum class transaction_command : char {
    begin,
    commit,
    rollback
};

struct transaction_statement : statement_t {
    transaction_statement(transaction_command command);
    ~transaction_statement() override;
    transaction_command command;
};

enum class  join_t { inner, full, left, right, cross, natural };

struct collection_ref_t {

};

struct join_definition_t {
    join_definition_t();
    virtual ~join_definition_t();

    collection_ref_t* left;
    collection_ref_t* right;
    /// Expr* condition;

    join_t type;
};