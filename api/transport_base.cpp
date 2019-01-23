#include <rocketjoe/api/transport_base.hpp>
#include <rocketjoe/api/websocket.hpp>
#include <rocketjoe/api/http.hpp>

namespace rocketjoe { namespace api {

    transport_base::transport_base(transport_type type,transport_id id ) : type_(type),id_(id) {}

        auto transport_base::type() -> transport_type {
            return type_;
        }

        auto transport_base::id() -> transport_id {
            return id_;
        }

        auto create_transport(transport_type type, transport_id id) -> transport {
            if(type  == transport_type::http){
                return make_transport<http>(id);
            }

            if(type  == transport_type::ws){
                return make_transport<web_socket>(id);
            }
        }

    }}