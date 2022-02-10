#pragma once

#include <string>
#include <vector>

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

enum class statement_type {
    error, // unused
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
    statement_t(statement_type type);

    virtual ~statement_t();

    statement_type type() const;

    bool isType(statement_type type) const;

    // Shorthand for isType(type).
    bool is(statement_type type) const;

    // Length of the string in the SQL query string
    std::size_t stringLength;

    std::vector<Expr*>* hints;

private:
    statement_type type_;
    std::string database_;
    std::string collection_;
    components::document::document_t document_;
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

struct find_t : statement_t {

    void to_msgpack(msgpack::object const& o){

    }
    template<typename Stream>
    void to_msgpack(msgpack::packer<Stream>& o){}
    void to_msgpack(msgpack::object::with_zone& o){}
};