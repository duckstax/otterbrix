#include "context.hpp"
#include "tracy/tracy.hpp"

auto session_handler_t::http_session(session_id id) const -> http_session_abstract_t* {
    ZoneScoped;
    std::shared_lock lock(http_ss_mutex_);
    auto session = http_session_storage_.find(id);
    if (session != http_session_storage_.end()) {
        return session->second.get();
    }
    return nullptr;
}

auto session_handler_t::ws_session(session_id id) const -> ws_session_abstract_t* {
    ZoneScoped;
    std::shared_lock lock(ws_ss_mutex_);
    auto session = ws_session_storage_.find(id);
    if (session != ws_session_storage_.end()) {
        return session->second.get();
    }
    return nullptr;
}

void session_handler_t::broadcast(std::string data) {
    ZoneScoped;
    std::unique_lock lock(ws_ss_mutex_);
    auto data_tmp = std::move(data);
    for (const auto& i : ws_session_storage_) {
        i.second->write(ws_message_ptr(new ws_string_message_t(std::move(data_tmp))));
    }
}

auto session_handler_t::close_session(session_id id) -> bool {
    ZoneScoped;
    log_.trace("{} :: +++", GET_TRACE());
    {
        std::unique_lock lock(http_ss_mutex_);
        auto http_it = http_session_storage_.find(id);
        if (http_it != http_session_storage_.end()) {
            http_session_storage_.erase(http_it);
            log_.trace("{} :: HTTP session erased", GET_TRACE());
            return true;
        }
    }
    {
        std::unique_lock lock(ws_ss_mutex_);
        auto ws_it = ws_session_storage_.find(id);
        if (ws_it != ws_session_storage_.end()) {
            ws_it->second->close();
            ws_session_storage_.erase(ws_it);
            log_.trace("{} :: WS session erased", GET_TRACE());
            return true;
        }
    }
    return false;
}
