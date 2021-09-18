#pragma once

#include <memory>

#include "beast-compact.hpp"
#include "context.hpp"

class detect_session_t : public std::enable_shared_from_this<detect_session_t> {
public:
    explicit detect_session_t(
        session_handler_t* session_handler,
        ssl::context& ssl_ctx,
        tcp::socket&& socket,
        log_t& log);

    ~detect_session_t();

    // Launch the detector
    void run();

private:
    void on_run();

    void on_detect(beast::error_code ec, bool result);

    beast::tcp_stream stream_;
    ssl::context& ssl_ctx_;
    beast::flat_buffer buffer_;
    session_handler_t* session_handler_;
    log_t log_;
};

auto make_detect_session(
    session_handler_t* session_handler,
    ssl::context& ssl_ctx,
    tcp::socket&& socket,
    log_t&) -> std::shared_ptr<detect_session_t>;
