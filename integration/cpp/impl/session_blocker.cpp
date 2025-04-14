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
        // it is possible that there is someone trying to create new session with the same id
        //! if this is a problem, solution will be to generate a new session
        auto it = insert_or_assign(session, value);
        if (!value && !it.second) {
            // if value == true, it is a return call and should be possible
            // if value == false and there is already a session here, then it should be illegal or fixed here
            throw std::runtime_error("session_block_t::set_value: session is already present");
        }
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