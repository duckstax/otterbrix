#include "ssl_websocket_session.hpp"

ssl_websocket_session_t::ssl_websocket_session_t(
    session_handler_t* session_handler,
    beast::ssl_stream<beast::tcp_stream>&& stream,
    log_t& log)
    : ws_(std::move(stream))
    , websocket_session_t<ssl_websocket_session_t>(session_handler, log) {
    ZoneScoped;
}

websocket::stream<beast::ssl_stream<beast::tcp_stream>>& ssl_websocket_session_t::ws() {
    ZoneScoped;
    return ws_;
}
