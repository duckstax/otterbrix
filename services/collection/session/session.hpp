#pragma once

#include <variant>
#include <components/session/session.hpp>
#include "session_type.hpp"
#include "create_index.hpp"
#include "suspend_plan.hpp"

namespace services::collection::sessions {

    class session_t {
    public:
        template<class T>
        explicit session_t(T&& statement)
            : type_(statement.type())
            , data_(std::move(statement))
        {}

        template<class T>
        T& get() {
            return std::get<T>(data_);
        }

        type_t type() const {
            return type_;
        }

    private:
        type_t type_;

        std::variant<
            create_index_t,
            suspend_plan_t
            > data_;
    };


    struct session_key_t {
        components::session::session_id_t id;
        std::string name;
    };

    inline bool operator==(const session_key_t& key1, const session_key_t& key2) {
        return key1.id == key2.id && key1.name == key2.name;
    }

    struct session_key_hash {
        std::size_t operator()(const session_key_t& key) const {
            return std::hash<components::session::session_id_t>()(key.id);
        }
    };

    using sessions_storage_t = std::unordered_map<session_key_t, session_t, session_key_hash>;

    template<typename T>
    void make_session(sessions_storage_t& storage, components::session::session_id_t session, T&& statement) {
        storage.emplace(session_key_t{session, {}}, session_t{statement});
    }

    template<typename T>
    void make_session(sessions_storage_t& storage, components::session::session_id_t session, const std::string& session_name, T&& statement) {
        storage.emplace(session_key_t{session, session_name}, session_t{statement});
    }

    inline session_t& find(sessions_storage_t& storage, components::session::session_id_t session) {
        auto it = storage.find(session_key_t{session, {}});
        return it->second;
    }

    inline session_t& find(sessions_storage_t& storage, components::session::session_id_t session, const std::string& session_name) {
        auto it = storage.find(session_key_t{session, session_name});
        return it->second;
    }

    inline void remove(sessions_storage_t& storage, components::session::session_id_t session) {
        storage.erase(session_key_t{session, {}});
    }

    inline void remove(sessions_storage_t& storage, components::session::session_id_t session, const std::string& session_name) {
        storage.erase(session_key_t{session, session_name});
    }

} // namespace services::collection::sessions
