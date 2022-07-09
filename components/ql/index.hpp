#pragma once

#include "ql_statement.hpp"

enum class index_type : char {
    default_,
    single,
    composite,
    multikey,
    hashed,
    wildcard,
};

struct create_index
    : ql_statement_t {
    create_index(const database_name_t& database, const collection_name_t& collection, index_type type)
        : ql_statement_t(statement_type::create_index, database, collection)
        , index_type_(type) {
    }
    std::vector<std::string> keys_;
    index_type index_type_;
};