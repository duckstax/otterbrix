#pragma once

#include <variant>

#include <actor-zeta.hpp>

#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>

class session_t {
public:
    explicit session_t(actor_zeta::address_t address);

    session_t(actor_zeta::address_t address,
              components::logical_plan::node_ptr node,
              components::logical_plan::parameter_node_ptr params)
        : address_(std::move(address))
        , data_(std::move(node))
        , params_(std::move(params)) {}

    actor_zeta::address_t address() { return address_; }

    components::logical_plan::node_ptr node() { return data_; }

    components::logical_plan::parameter_node_ptr params() { return params_; }

    template<typename T>
    bool is_type() const {
        return std::holds_alternative<T>(data_);
    }

    components::logical_plan::node_type type() const;

private:
    actor_zeta::address_t address_;
    components::logical_plan::node_ptr data_;
    components::logical_plan::parameter_node_ptr params_;
    ///components::session::session_id_t session_;
};

// TODO Remove this session and use logic from collections
// Move session logic from collections to a separate place
struct session_key_t {
    components::session::session_id_t id;
};

inline bool operator==(const session_key_t& key1, const session_key_t& key2) { return key1.id == key2.id; }

struct session_key_hash {
    std::size_t operator()(const session_key_t& key) const {
        return std::hash<components::session::session_id_t>()(key.id);
    }
};

using session_storage_t = std::unordered_map<session_key_t, session_t, session_key_hash>;

template<class... Args>
auto make_session(session_storage_t& storage, components::session::session_id_t session, Args&&... args) -> void {
    storage.emplace(session_key_t{session}, session_t(std::forward<Args>(args)...));
}

template<class... Args>
auto make_session(session_storage_t& storage, const session_key_t& session, Args&&... args) -> void {
    storage.emplace(session, session_t(std::forward<Args>(args)...));
}

inline auto find_session(session_storage_t& storage, components::session::session_id_t session) -> session_t& {
    auto it = storage.find(session_key_t{session});
    assert(it != std::end(storage) && "it != std::end(storage)");
    return it->second;
}

inline auto is_session_exist(session_storage_t& storage, components::session::session_id_t session) -> bool {
    auto it = storage.find(session_key_t{session});
    return it != std::end(storage);
}

inline auto remove_session(session_storage_t& storage, components::session::session_id_t session) -> void {
    storage.erase(session_key_t{session});
}
