#include "session.hpp"

using namespace components::logical_plan;

session_t::session_t(actor_zeta::address_t address)
    : address_(std::move(address))
    , data_(nullptr) {}

node_type session_t::type() const {
    if (data_) {
        return data_->type();
    } else {
        return node_type::unused;
    }
}
