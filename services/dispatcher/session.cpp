#include "session.hpp"

session_t::session_t(goblin_engineer::address_t address)
    :address_(std::move(address)){}
