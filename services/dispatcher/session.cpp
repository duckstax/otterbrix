#include "session.hpp"

session_t::session_t(actor_zeta::address_t address)
    :address_(std::move(address)){}

statement_type session_t::type() const {
    return std::visit([](const auto &c) {
        using type = std::decay_t<decltype(c)>;
        if constexpr (std::is_same_v<type, insert_one_t>) {
            return statement_type::insert_one;
        } else if constexpr (std::is_same_v<type, insert_many_t>) {
            return statement_type::insert_many;
        } else if constexpr (std::is_same_v<type, delete_one_t>) {
            return statement_type::delete_one;
        } else if constexpr (std::is_same_v<type, delete_many_t>) {
            return statement_type::delete_many;
        } else if constexpr (std::is_same_v<type, update_one_t>) {
            return statement_type::update_one;
        } else if constexpr (std::is_same_v<type, update_many_t>) {
            return statement_type::update_many;
        }
        static_assert(true, "Not valid command type");
    }, data_);
}
