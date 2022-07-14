#pragma once

#include "ql_statement.hpp"

namespace components::ql {

    enum class index_type : char {
        default_,
        single,
        composite,
        multikey,
        hashed,
        wildcard,
    };

    struct create_index_t
        : ql_statement_t {
        create_index_t(const database_name_t& database, const collection_name_t& collection, index_type type)
            : ql_statement_t(statement_type::create_index, database, collection)
            , index_type_(type) {
        }
        std::vector<std::string> keys_;
        index_type index_type_;
    };
} // namespace components::ql