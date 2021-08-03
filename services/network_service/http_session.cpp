#include "http_session.hpp"

#include "plain_websocket_session.hpp"
#include "ssl_websocket_session.hpp"

auto make_websocket_session(
    session_handler_t* session_handler,
    beast::tcp_stream stream,
    log_t& log) -> std::shared_ptr<plain_websocket_session_t> {
    ZoneScoped;
    return std::make_shared<plain_websocket_session_t>(session_handler, std::move(stream), log);
}
auto make_websocket_session(
    session_handler_t* session_handler,
    beast::ssl_stream<beast::tcp_stream> stream,
    log_t& log) -> std::shared_ptr<ssl_websocket_session_t> {
    ZoneScoped;
    return std::make_shared<ssl_websocket_session_t>(session_handler, std::move(stream), log);
}
