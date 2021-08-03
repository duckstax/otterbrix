#include "detect_session.hpp"

#include "plain_http_session.hpp"
#include "session.hpp"
#include "ssl_http_session.hpp"

detect_session_t::detect_session_t(
    session_handler_t* session_handler,
    ssl::context& ssl_ctx,
    tcp::socket&& socket,
    log_t& log)
    : stream_(std::move(socket))
    , ssl_ctx_(ssl_ctx)
    , session_handler_(session_handler)
    , log_(log.clone()) {
    ZoneScoped;
}

detect_session_t::~detect_session_t() {
    ZoneScoped;
    log_.trace("{} DESTRUCTOR", GET_TRACE());
}

void detect_session_t::run() {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    net::dispatch(
        stream_.get_executor(),
        beast::bind_front_handler(
            &detect_session_t::on_run,
            this->shared_from_this()));
    log_.trace("{} ---", GET_TRACE());
}

void detect_session_t::on_run() {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    // Set the timeout.
    ///stream_.expires_after(std::chrono::seconds(30));

    beast::async_detect_ssl(
        stream_,
        buffer_,
        beast::bind_front_handler(
            &detect_session_t::on_detect,
            this->shared_from_this()));
    log_.trace("{} ---", GET_TRACE());
}

void detect_session_t::on_detect(beast::error_code ec, bool result) {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    if (ec) {
        log_.error("{} : !!! {} !!!", GET_TRACE(), ec.message());
        return;
    }

    if (result) {
        // Launch SSL session
        session_handler_->http_session(
                make_ssl_http_session(
                    session_handler_,
                    ssl_ctx_,
                    std::move(stream_),
                    std::move(buffer_), log_))
            .run();
        log_.trace("{} ---", GET_TRACE());
        return;
    }

    // Launch plain session
    session_handler_->http_session(
            make_plain_http_session(
                session_handler_,
                std::move(stream_),
                std::move(buffer_), log_))
        .run();
    log_.trace("{} ---", GET_TRACE());
}

auto make_detect_session(
    session_handler_t* session_handler,
    ssl::context& ssl_ctx,
    tcp::socket&& socket,
    log_t& log) -> std::shared_ptr<detect_session_t> {
    ZoneScoped;
    return std::make_shared<detect_session_t>(session_handler, ssl_ctx, std::move(socket), log);
}
