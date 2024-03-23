#pragma once

#include "ql_statement.hpp"

namespace components::ql {

    enum class transaction_command : char
    {
        begin,
        commit,
        rollback
    };

    struct transaction_statement : ql_statement_t {
        transaction_statement(transaction_command command);
        ~transaction_statement() override;
        transaction_command command;
    };
} // namespace components::ql