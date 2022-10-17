#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include "expr.hpp"
#include "ql_statement.hpp"

namespace components::ql {

    struct find_statement final : public ql_statement_t {
        find_statement(database_name_t database, collection_name_t collection, expr_ptr &&condition, bool is_find_one);
        expr_ptr condition_;
    };

    using find_statement_ptr = boost::intrusive_ptr<find_statement>;

    find_statement_ptr make_find_statement(database_name_t database, collection_name_t collection, expr_ptr &&condition, bool is_find_one);

} // namespace components::ql