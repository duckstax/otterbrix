#include "session_blocker.hpp"

namespace otterbrix::impl {

    using components::session::session_id_t;

    session_block_t::session_block_t(std::pmr::memory_resource* resource)
        : std::pmr::unordered_map<session_id_t, bool>(resource) {}

    bool session_block_t::empty() noexcept {
        std::lock_guard lock(read_mutex_);
        return std::pmr::unordered_map<session_id_t, bool>::empty();
    }

    size_t session_block_t::size() noexcept {
        std::lock_guard lock(read_mutex_);
        return std::pmr::unordered_map<session_id_t, bool>::size();
    }

    void session_block_t::clear() noexcept {
        std::lock_guard lock(write_mutex_);
        return std::pmr::unordered_map<session_id_t, bool>::clear();
    }

    void session_block_t::set_value(const session_id_t& session, bool value) {
        std::lock_guard lock(write_mutex_);
        insert_or_assign(session, value);
    }

    void session_block_t::remove_session(const session_id_t& session) {
        std::lock_guard lock(write_mutex_);
        erase(session);
    }

    bool session_block_t::value(const session_id_t& session) {
        std::lock_guard lock(read_mutex_);
        return std::pmr::unordered_map<session_id_t, bool>::at(session);
    }
} // namespace otterbrix::impl