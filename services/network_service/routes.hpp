#pragma once

namespace network_service_routes {

    static constexpr auto name = "network_service";
    static constexpr auto http_write = "http_write";
    static constexpr auto ws_write = "ws_write";
    static constexpr auto http_client_write = "http_client_write";
    static constexpr auto dispatch_output = "dispatch_output";
    static constexpr auto dispatcher = "dispatcher";
    static constexpr auto dispatcher_py = "dispatcher_py";
    static constexpr auto http_dispatch = "http_dispatch";
    static constexpr auto ws_dispatch = "ws_dispatch";
    static constexpr auto ws_client_dispatch = "ws_client_dispatch";
    static constexpr auto close_session = "close_session";

} // namespace network_service_routes
