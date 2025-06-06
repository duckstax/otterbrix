#pragma once

#include <components/session/session.hpp>
#include <memory_resource>
#include <shared_mutex>
#include <unordered_map>

namespace otterbrix::impl {
    class session_block_t : private std::pmr::unordered_map<components::session::session_id_t, bool> {
    public:
        explicit session_block_t(std::pmr::memory_resource* resource);

        bool empty() noexcept;
        size_t size() noexcept;
        void clear() noexcept;
        // return false if there is a hash conflict
        bool set_value(const components::session::session_id_t& session, bool value);
        void remove_session(const components::session::session_id_t& session);
        bool value(const components::session::session_id_t& session);

    private:
        std::shared_mutex mutex_;
    };
} // namespace otterbrix::impl