#pragma once

#include <actor-zeta.hpp>
#include <utility>
#include "session_type.hpp"

namespace services::collection::sessions {

    struct create_index : public session_base_t<create_index> {
        actor_zeta::address_t client;
        uint32_t id_index;

        create_index(actor_zeta::address_t client, uint32_t id_index)
            : client(std::move(client))
            , id_index(id_index)
        {}

        static type_t type_impl() {
            return type_t::create_index;
        }
    };

} // namespace services::collection::sessions