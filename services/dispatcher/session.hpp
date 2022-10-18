#pragma once

#include <variant>

#include <actor-zeta.hpp>

#include <components/ql/statements.hpp>
#include <components/session/session.hpp>

class session_t {
public:
    explicit session_t(actor_zeta::address_t address);

    template<class T>
    session_t(actor_zeta::address_t address, T statement)
        : address_(std::move(address))
        , data_(std::forward<T>(statement)) {}

    actor_zeta::address_t address() {
        return address_;
    }

    template<class T>
    T& get() {
        return std::get<T>(data_);
    }

    components::ql::statement_type type() const;

private:
    actor_zeta::address_t address_;
    components::ql::variant_statement_t data_;
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