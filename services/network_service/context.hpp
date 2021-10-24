#pragma once

#include <map>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include <boost/asio/ip/tcp.hpp>

#include <goblin-engineer/core.hpp>

#include "beast-compact.hpp"
#include "tracy/tracy.hpp"
#include "http_client.hpp"
#include "routes.hpp"
#include "session.hpp"

namespace net = boost::asio;

using tcp = boost::asio::ip::tcp;
using session_id = std::uintptr_t;
using addres_book = std::function<actor_zeta::actor_address(actor_zeta::detail::string_view)>;

class plain_websocket_client_session_t;
class plain_websocket_session_t;
class ssl_websocket_session_t;
class plain_http_session_t;
class ssl_http_session_t;

class session_handler_t {
public:
    template<class F>
    session_handler_t(
        F f,
        const std::string& dispatcher_route,
        const std::string& http_dispatch_method,
        const std::string& ws_dispatch_method,
        log_t& log)
        : address_book_(std::forward<F>(f))
        , dispatcher_route_(dispatcher_route)
        , http_dispatch_method_(http_dispatch_method)
        , ws_dispatch_method_(ws_dispatch_method)
        , log_(log.clone()) {
        ZoneScoped;
        self_ = address_book_("self");
    }
    ~session_handler_t() = default;

    template<typename... Args>
    void http_dispatch(Args... args) {
        ZoneScoped;
        actor_zeta::send(
            address_book_(dispatcher_route_), self_, http_dispatch_method_, std::forward<Args>(args)...);
    }

    //NOTE: signature is session_id, std::string&, size_t
    template<typename... Args>
    void ws_dispatch(Args... args) {
        ZoneScoped;
        actor_zeta::send(
            address_book_(dispatcher_route_), self_, ws_dispatch_method_, std::forward<Args>(args)...);
    }

    template<typename... Args>
    void close(Args... args) {
        ZoneScoped;
        actor_zeta::send(self_, self_, network_service_routes::close_session, std::forward<Args>(args)...);
    }

    template<class Session>
    auto http_session(std::shared_ptr<Session> session) -> Session& {
        ZoneScoped;
        std::unique_lock lock(http_ss_mutex_);
        log_.trace("{} :: HTTP session document's size: {}", GET_TRACE(), http_session_storage_.size());
        auto tmp = std::move(session);
        auto* ptr = tmp.get();
        auto id = reinterpret_cast<std::uintptr_t>(ptr);
        http_session_storage_.emplace(id, std::static_pointer_cast<http_session_abstract_t>(std::move(tmp)));
        return *ptr;
    }

    template<class Session>
    auto ws_session(std::shared_ptr<Session> session) -> Session& {
        ZoneScoped;
        std::unique_lock lock(ws_ss_mutex_);
        log_.trace("{} :: WS session document's size: {}", GET_TRACE(), ws_session_storage_.size());
        auto tmp = std::move(session);
        auto* ptr = tmp.get();
        auto id = reinterpret_cast<std::uintptr_t>(ptr);
        ws_session_storage_.emplace(id, std::static_pointer_cast<ws_session_abstract_t>(std::move(tmp)));
        return *ptr;
    }

    auto http_session(session_id id) const -> http_session_abstract_t*;

    auto ws_session(session_id id) const -> ws_session_abstract_t*;

    void broadcast(std::string data);

    auto close_session(session_id id) -> bool;

    /*auto io_context_read() -> net::io_context::strand& {
        return ioc_read_;
    }

    auto io_context_write() -> net::io_context::strand& {
        return ioc_write_;
    }*/

private:
    addres_book address_book_;
    std::string dispatcher_route_;
    std::string http_dispatch_method_;
    std::string ws_dispatch_method_;
    log_t log_;
    actor_zeta::actor_address self_;

    mutable std::shared_mutex http_ss_mutex_;
    std::unordered_map<session_id, std::shared_ptr<http_session_abstract_t>> http_session_storage_;

    mutable std::shared_mutex ws_ss_mutex_;
    std::unordered_map<session_id, std::shared_ptr<ws_session_abstract_t>> ws_session_storage_;

    //net::io_context::strand ioc_read_;
    //net::io_context::strand ioc_write_;
};

using session_handler_ptr = std::unique_ptr<session_handler_t>;
