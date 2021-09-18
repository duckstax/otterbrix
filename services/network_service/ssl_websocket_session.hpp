#pragma once

#include <memory>

#include "beast-compact.hpp"
#include "websocket_session.hpp"

class ssl_websocket_session_t
    : public websocket_session_t<ssl_websocket_session_t>
    , public std::enable_shared_from_this<ssl_websocket_session_t> {
public:
    explicit ssl_websocket_session_t(
        session_handler_t* session_handler,
        beast::ssl_stream<beast::tcp_stream>&& stream,
        log_t&);

    // Called by the base class
    websocket::stream<beast::ssl_stream<beast::tcp_stream>>&
    ws();

private:
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
};