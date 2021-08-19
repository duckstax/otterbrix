#include "dto.hpp"
#include "tracy/tracy.hpp"
#include <apps/medusa_cascade/protocol/protocol.hpp>
#include <variant>

void make_http_client_request(
    const std::string& host,
    const std::string& target,
    http::verb method,
    request_t& request) {
    ZoneScoped;
    request.method(method);
    request.version(11);
    request.target(target);
    request.set(http::field::host, host);
}

template class ws_message_base_t<ws_message_type::strdata_t>;
template class ws_message_base_t<ws_message_type::bindata_t>;
