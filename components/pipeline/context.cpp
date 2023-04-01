#include "context.hpp"

namespace components::pipeline {

    context_t::context_t(ql::storage_parameters* init_parameters)
        : parameters(init_parameters) {}

    context_t::context_t(session::session_id_t session, actor_zeta::address_t address, ql::storage_parameters* init_parameters)
        : session(session)
        , parameters(init_parameters)
        , address_(std::move(address)) {}

} // namespace components::pipeline
