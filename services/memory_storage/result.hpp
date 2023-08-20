#pragma once

#include <actor-zeta.hpp>
#include <components/ql/ql_statement.hpp>

namespace services::memory_storage {

    template <typename TResult>
    struct result_t final {
        bool success;
        components::ql::ql_statement_t* input_statement;
        TResult result;

        result_t(bool success, components::ql::ql_statement_t* input_statement, const TResult& result)
            : success(success)
            , input_statement(input_statement)
            , result(result) {
        }
    };

    struct empty_result_t {
    };

    struct result_address_t {
        actor_zeta::address_t address;
    };

} // namespace services::memory_storage
