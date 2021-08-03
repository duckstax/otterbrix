#pragma once

#include <memory>

#include "beast-compact.hpp"
#include "context.hpp"
#include "websocket_session.hpp"

class plain_websocket_client_session_t
    : public websocket_client_session_t<plain_websocket_client_session_t>
    , public std::enable_shared_from_this<plain_websocket_client_session_t> {
public:
    // Create the session
    explicit plain_websocket_client_session_t(
        session_handler_t* session_handler,
        net::io_context& ioc,
        log_t& log)
        : resolver_(net::make_strand(ioc))
        , ws_(net::make_strand(ioc))
        , websocket_client_session_t<plain_websocket_client_session_t>(session_handler, log) {
        ZoneScoped;
    }

    // Called by the base class
    tcp::resolver& resolver();
    websocket::stream<beast::tcp_stream>& ws();

private:
    tcp::resolver resolver_;
    websocket::stream<beast::tcp_stream> ws_;
};

auto make_websocket_client_session(
    session_handler_t* session_handler,
    net::io_context& ioc,
    log_t& log) -> std::shared_ptr<plain_websocket_client_session_t>;

class plain_websocket_session_t
    : public websocket_session_t<plain_websocket_session_t>
    , public std::enable_shared_from_this<plain_websocket_session_t> {
public:
    // Create the session
    explicit plain_websocket_session_t(
        session_handler_t* session_handler,
        beast::tcp_stream&& stream,
        log_t& log)
        : ws_(std::move(stream))
        , websocket_session_t<plain_websocket_session_t>(session_handler, log) {
        ZoneScoped;
    }

    // Called by the base class
    websocket::stream<beast::tcp_stream>&
    ws();

private:
    websocket::stream<beast::tcp_stream> ws_;
};
