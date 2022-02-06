#pragma once

#include <components/session/session.hpp>
#include <goblin-engineer/core.hpp>

namespace services::storage {
    using components::session::session_t;
    class database_t;
    using database_ptr = goblin_engineer::intrusive_ptr<database_t>;
} // namespace services::storage