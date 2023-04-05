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


    using sessions_storage_t =  std::unordered_map<components::session::session_id_t, session_t>;

    template<typename T>
    void make_session(sessions_storage_t& storage, components::session::session_id_t session, T&& statement) {
        storage.emplace(session, session_t{statement});
    }

    inline session_t& find(sessions_storage_t& storage, components::session::session_id_t session) {
        auto it = storage.find(session);
        return it->second;
    }

    inline void remove(sessions_storage_t& storage,components::session::session_id_t session) {
        storage.erase(session);
    }

} // namespace services::collection::sessions
