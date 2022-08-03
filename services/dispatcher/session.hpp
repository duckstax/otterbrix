#pragma once

#include <actor-zeta.hpp>

#include <protocol/protocol.hpp>

#include <components/session/session.hpp>
#include <utility>

class session_t {
public:
    explicit session_t(actor_zeta::address_t address);

    template<class T>
    session_t(actor_zeta::address_t address, T statement)
        : address_(address)
        , data_(std::forward<T>(statement)) {}

    actor_zeta::address_t address() {
        return address_;
    }

    template<class T>
    T& get() {
        return std::get<T>(data_);
    }

    statement_type type() const;

private:
    actor_zeta::address_t address_;
    std::variant<components::ql::insert_one_t,
                 components::ql::insert_many_t,
                 components::ql::delete_one_t,
                 components::ql::delete_many_t,
                 components::ql::update_one_t,
                 components::ql::update_many_t,
                 components::ql::create_index_t>
        data_;
    ///components::session::session_id_t session_;
};


using session_storage_t =  std::unordered_map<components::session::session_id_t, session_t>;

template<class ...Args>
auto make_session(session_storage_t& storage ,components::session::session_id_t session,Args&&...args) -> void {
    storage.emplace(session,session_t(std::forward<Args>(args)...));
}

inline auto find(session_storage_t& storage,components::session::session_id_t session) -> session_t& {
    auto it = storage.find(session);
    return  it ->second;
}

inline  auto remove(session_storage_t& storage,components::session::session_id_t session) -> void  {
    storage.erase(session);
}