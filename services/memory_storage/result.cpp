#include "result.hpp"

namespace services::memory_storage {

    error_result_t::error_result_t(const std::string& what)
        : what(what) {
    }


    result_address_t::result_address_t(actor_zeta::base::address_t&& address)
        : address(std::move(address)) {
    }


    result_t::result_t(components::ql::ql_statement_t* input_statement, error_result_t&& result)
        : input_statement_(input_statement)
        , result_(std::move(result))
        , is_error_(true) {
    }

    bool result_t::is_error() const {
        return is_error_;
    }

    bool result_t::is_success() const {
        return !is_error_;
    }

    const std::string& result_t::error_what() const {
        return result<error_result_t>().what;
    }

    components::ql::ql_statement_t* result_t::input_statement() const {
        return input_statement_;
    }


    result_t make_error(components::ql::ql_statement_t* input_statement, const std::string& error) {
        return result_t{input_statement, error_result_t(error)};
    }

} // namespace services::memory_storage
