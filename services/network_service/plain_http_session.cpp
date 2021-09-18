#include "plain_http_session.hpp"

plain_http_session_t::plain_http_session_t(
    session_handler_t* session_handler,
    beast::tcp_stream&& stream,
    beast::flat_buffer&& buffer,
    log_t& log)
    : http_session_t<plain_http_session_t>(session_handler, std::move(buffer), log)
    , stream_(std::move(stream))
    , session_handler_(session_handler) {
    ZoneScoped;
}

void plain_http_session_t::run() {
    ZoneScoped;
    this->do_read();
}

beast::tcp_stream& plain_http_session_t::stream() {
    ZoneScoped;
    return stream_;
}

beast::tcp_stream plain_http_session_t::release_stream() {
    ZoneScoped;
    return std::move(stream_);
}

void plain_http_session_t::do_eof() {
    ZoneScoped;
    // Send a TCP shutdown
    beast::error_code ec;
    stream_.socket().shutdown(tcp::socket::shutdown_send, ec);

    // At this point the connection is closed gracefully
}

using plain_http_session_ptr = std::shared_ptr<plain_http_session_t>;

auto make_plain_http_session(
    session_handler_t* session_handler,
    beast::tcp_stream&& stream,
    beast::flat_buffer&& buffer,
    log_t& log) -> plain_http_session_ptr {
    ZoneScoped;
    return std::make_shared<plain_http_session_t>(session_handler, std::move(stream), std::move(buffer), log);
}
