#pragma once

#include <excutor.hpp>

#include <boost/uuid/uuid.hpp>

#include <goblin-engineer/core.hpp>


namespace services::storage {
    using session_id = std::uintptr_t;

    struct session_t final {
        session_t()=default;
        session_id id_;
        boost::uuids::uuid uid_;
    };
    class database_t;
    using database_ptr = goblin_engineer::intrusive_ptr<database_t>;
} // namespace kv