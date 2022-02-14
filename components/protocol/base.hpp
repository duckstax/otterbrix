#pragma once

#include <string>
#include <vector>
#include <variant>

#include <components/document/document.hpp>

enum class DataType {
    UNKNOWN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    FLOAT,
    INT,
    LONG,
    REAL,
    SMALLINT,
    TEXT,
    TIME,
    VARCHAR,
};

enum class statement_type : char {
    error = 0x00, // unused
    create_database,
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
    statement_t(statement_type type, size_t size , const std::string& database, const std::string& collection)
        : type_(type)
        , size_(size+init_size_)
        , database_(database)
        , collection_(collection) {}

    virtual ~statement_t();

    ///    std::vector<Expr*>* hints;
    size_t size() {
        return size_;
    }


    statement_type type_;
    std::string database_;
    std::string collection_;
private:
    const size_t init_size_ {2};
    const size_t size_ ;
};

enum class transaction_command : char {
    Begin,
    Commit,
    Rollback
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