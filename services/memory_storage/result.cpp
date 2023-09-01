#include "result.hpp"

namespace services::memory_storage {

    error_result_t::error_result_t(error_code_t code, const std::string& what)
        : code(code)
        , what(what) {
    }


    result_address_t::result_address_t(actor_zeta::base::address_t&& address)
        : address(std::move(address)) {
    }


    result_list_addresses_t::result_list_addresses_t(std::pmr::memory_resource* resource)
        : addresses(resource) {
    }


    result_t::result_t(error_result_t&& result)
        : result_(std::move(result))
        , is_error_(true) {
    }

    bool result_t::is_error() const {
        return is_error_;
    }

    bool result_t::is_success() const {
        return !is_error_;
    }

    error_code_t result_t::error_code() const {
        return result<error_result_t>().code;
    }

    const std::string& result_t::error_what() const {
        return result<error_result_t>().what;
    }


    result_t make_error(error_code_t code, const std::string& error) {
        return result_t{error_result_t(code, error)};
    }

} // namespace services::memory_storage
