#include <rocketjoe/api/websocket.hpp>
#include "websocket.hpp"


namespace rocketjoe { namespace api {

        web_socket::web_socket(transport_id id) :transport_base(transport_type::ws,id) {}

    }}