#include "session.hpp"

session_t::session_t(actor_zeta::address_t address)
    :address_(std::move(address)){}
