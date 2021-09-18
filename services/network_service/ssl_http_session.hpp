#pragma once

#include <memory>

#include "beast-compact.hpp"

#include "context.hpp"
#include "http_session.hpp"

class ssl_http_session_t
    : public http_session_t<ssl_http_session_t>
    , public std::enable_shared_from_this<ssl_http_session_t> {
    beast::ssl_stream<beast::tcp_stream> stream_;

public:
    ssl_http_session_t(
        session_handler_t* session_handler,
        ssl::context& ssl_ctx,
        beast::tcp_stream&& stream,
        beast::flat_buffer&& buffer, log_t&);

    // Start the session
    void
    run();

    // Called by the base class
    beast::ssl_stream<beast::tcp_stream>&
    stream();

    // Called by the base class
    beast::ssl_stream<beast::tcp_stream>
    release_stream();

    // Called by the base class
    void
    do_eof();

private:
    void
    on_handshake(
        beast::error_code ec,
        std::size_t bytes_used);

    void
    on_shutdown(beast::error_code ec);
};

auto make_ssl_http_session(
    session_handler_t* session_handler,
    ssl::context& ssl_ctx,
    beast::tcp_stream&& stream,
    beast::flat_buffer&& buffer, log_t&) -> std::shared_ptr<ssl_http_session_t>;