#pragma once

#include <excutor.hpp>
#include <goblin-engineer/core.hpp>

namespace services::storage {
    using session_id_t = std::uintptr_t;
    class database_t;
    using database_ptr = goblin_engineer::intrusive_ptr<database_t>;
} // namespace kv