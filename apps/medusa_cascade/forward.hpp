#pragma once

#include <RocketJoe/core/excutor.hpp>
#include <actor-zeta.hpp>

namespace kv {

    using session_id_t = std::uintptr_t;
    class database_t;
    using database_ptr = std::unique_ptr<database_t>;
} // namespace kv