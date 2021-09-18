#include "plain_websocket_session.hpp"

tcp::resolver& plain_websocket_client_session_t::resolver() {
    ZoneScoped;
    return resolver_;
}

websocket::stream<beast::tcp_stream>& plain_websocket_client_session_t::ws() {
    ZoneScoped;
    return ws_;
}

auto make_websocket_client_session(
    session_handler_t* session_handler,
    net::io_context& ioc,
    log_t& log) -> std::shared_ptr<plain_websocket_client_session_t> {
    ZoneScoped;
    return std::make_shared<plain_websocket_client_session_t>(session_handler, ioc, log);
}

websocket::stream<beast::tcp_stream>& plain_websocket_session_t::ws() {
    ZoneScoped;
    return ws_;
}
