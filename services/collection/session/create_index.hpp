#pragma once

#include "session_type.hpp"
#include <actor-zeta.hpp>
#include <utility>

namespace services::collection::sessions {

    struct create_index_t : public session_base_t<create_index_t> {
        actor_zeta::address_t client;
        uint32_t id_index;
        bool is_pending;

        create_index_t(actor_zeta::address_t client, uint32_t id_index, bool is_pending = false)
            : client(std::move(client))
            , id_index(id_index)
            , is_pending(is_pending) {}

        static type_t type_impl() { return type_t::create_index; }
    };

} // namespace services::collection::sessions