#pragma once

#include <goblin-engineer/core.hpp>

#include <protocol/insert_one.hpp>
#include <protocol/insert_many.hpp>

#include <components/session/session.hpp>
#include <utility>

class session_t {
public:
    explicit session_t(goblin_engineer::address_t address);

    template<class T>
    session_t(goblin_engineer::address_t address,T statment )
        :address_(address)
        , data_(std::forward<T>(statment)){}

    goblin_engineer::address_t address() {
        return address_;
    }

    template<class T>
    T& get() {
        return std::get<T>(data_);
    }

private:
    goblin_engineer::address_t address_;
    std::variant<insert_one_t,
                 insert_many_t>
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