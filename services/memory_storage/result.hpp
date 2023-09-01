#pragma once

#include <memory_resource>
#include <variant>
#include <actor-zeta.hpp>
#include <components/ql/ql_statement.hpp>

namespace services::memory_storage {

    enum class error_code_t {
        database_already_exists,
        database_not_exists,
        collection_already_exists,
        collection_not_exists,
        other_error
    };

    struct empty_result_t {
    };


    struct error_result_t {
        error_code_t code;
        std::string what;

        explicit error_result_t(error_code_t code, const std::string& what);
    };


    struct result_address_t {
        actor_zeta::address_t address{actor_zeta::address_t::empty_address()};

        result_address_t() = default;
        explicit result_address_t(actor_zeta::address_t&& address);
    };

    struct result_list_addresses_t {
        struct res_t {
            collection_full_name_t name;
            actor_zeta::address_t address;
        };

        std::pmr::vector<res_t> addresses;

        explicit result_list_addresses_t(std::pmr::memory_resource *resource);
    };


    class result_t {
    public:
        explicit result_t(error_result_t&& result);

        template <typename T>
        explicit result_t(T&& result)
            : result_(std::move(result))
            , is_error_(false) {
        }

        bool is_error() const;
        bool is_success() const;
        error_code_t error_code() const;
        const std::string& error_what() const;

        template <typename T>
        const T& result() const {
            return std::get<T>(result_);
        }

    private:
        std::variant<
            error_result_t,
            empty_result_t,
            result_address_t,
            result_list_addresses_t
        > result_;
        bool is_error_;
    };


    template <typename TResult>
    result_t make_result(const TResult& result) {
        return result_t{std::move(result)};
    }

    result_t make_error(error_code_t code, const std::string& error);

} // namespace services::memory_storage
