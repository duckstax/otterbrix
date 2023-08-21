#pragma once

#include <actor-zeta.hpp>
#include <components/ql/ql_statement.hpp>

namespace services::memory_storage {

    using error_t = std::string;

    template <typename TResult>
    struct result_t final {
        bool success;
        components::ql::ql_statement_t* input_statement;
        TResult result;
        error_t error;
    };

    template <typename TResult>
    result_t<TResult> make_result(components::ql::ql_statement_t* input_statement, const TResult& result) {
        result_t<TResult> res;
        res.success = true;
        res.input_statement = input_statement;
        res.result = result;
        return res;
    }

    template <typename TResult>
    result_t<TResult> make_error(components::ql::ql_statement_t* input_statement, const TResult& result, const error_t& error) {
        result_t<TResult> res;
        res.success = false;
        res.input_statement = input_statement;
        res.result = result;
        res.error = error;
        return res;
    }

    struct empty_result_t {
    };

    struct result_address_t {
        actor_zeta::address_t address{actor_zeta::address_t::empty_address()};

        result_address_t() = default;
        explicit result_address_t(actor_zeta::address_t&& address)
            : address(std::move(address)) {
        }
    };

} // namespace services::memory_storage
