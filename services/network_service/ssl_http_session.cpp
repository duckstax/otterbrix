#include "ssl_http_session.hpp"

ssl_http_session_t::ssl_http_session_t(
    session_handler_t* session_handler,
    ssl::context& ssl_ctx,
    beast::tcp_stream&& stream,
    beast::flat_buffer&& buffer,
    log_t& log)
    : http_session_t<ssl_http_session_t>(session_handler, std::move(buffer), log)
    , stream_(std::move(stream), ssl_ctx) {
    ZoneScoped;
}

void ssl_http_session_t::run() {
    ZoneScoped;
    // Set the timeout.
    beast::get_lowest_layer(stream_).expires_after(std::chrono::seconds(30));

    // Perform the SSL handshake
    // Note, this is the buffered version of the handshake.
    stream_.async_handshake(
        ssl::stream_base::server,
        buffer_.data(),
        beast::bind_front_handler(
            &ssl_http_session_t::on_handshake,
            shared_from_this()));
}

beast::ssl_stream<beast::tcp_stream>& ssl_http_session_t::stream() {
    ZoneScoped;
    return stream_;
}

beast::ssl_stream<beast::tcp_stream> ssl_http_session_t::release_stream() {
    ZoneScoped;
    return std::move(stream_);
}

void ssl_http_session_t::do_eof() {
    ZoneScoped;
    // Set the timeout.
    beast::get_lowest_layer(stream_).expires_after(std::chrono::seconds(30));

    // Perform the SSL shutdown
    stream_.async_shutdown(
        beast::bind_front_handler(
            &ssl_http_session_t::on_shutdown,
            shared_from_this()));
}

void ssl_http_session_t::on_handshake(beast::error_code ec, std::size_t bytes_used) {
    ZoneScoped;
    if (ec)
        return log_.error("handshake : {}", ec.message());

    // Consume the portion of the buffer used by the handshake
    buffer_.consume(bytes_used);

    do_read();
}

void ssl_http_session_t::on_shutdown(beast::error_code ec) {
    ZoneScoped;
    if (ec)
        return log_.error("shutdown : {}", ec.message());

    // At this point the connection is closed gracefully
}
auto make_ssl_http_session(
    session_handler_t* session_handler,
    ssl::context& ssl_ctx,
    beast::tcp_stream&& stream,
    beast::flat_buffer&& buffer,
    log_t& log) -> std::shared_ptr<ssl_http_session_t> {
    ZoneScoped;
    return std::make_shared<ssl_http_session_t>(session_handler, ssl_ctx, std::move(stream), std::move(buffer), log);
}
