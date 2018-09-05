#include <rocketjoe/api/websocket.hpp>
#include "websocket.hpp"


namespace RocketJoe { namespace transport {

        web_socket::web_socket(transport_id id) :transport_base(transport_type::ws,id) {}

        web_socket::~web_socket() = default;
    }}