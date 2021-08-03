#pragma once

#include <memory>

#include "beast-compact.hpp"

#include "http_session.hpp"

class plain_http_session_t
    : public http_session_t<plain_http_session_t>
    , public std::enable_shared_from_this<plain_http_session_t> {
    beast::tcp_stream stream_;
    session_handler_t* session_handler_;

public:
    // Create the session
    plain_http_session_t(
        session_handler_t* session_handler,
        beast::tcp_stream&& stream,
        beast::flat_buffer&& buffer, log_t& log);

    // Start the session
    void run();

    // Called by the base class
    beast::tcp_stream& stream();

    // Called by the base class
    beast::tcp_stream release_stream();

    // Called by the base class
    void do_eof();
};

auto make_plain_http_session(
    session_handler_t* session_handler,
    beast::tcp_stream&& stream,
    beast::flat_buffer&& buffer,
    log_t&) -> std::shared_ptr<plain_http_session_t>;
