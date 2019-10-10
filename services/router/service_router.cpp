#include <rocketjoe/services/router/service_router.hpp>

#include <unordered_map>

#include <goblin-engineer.hpp>

#include <rocketjoe/services/router/router.hpp>
#include <rocketjoe/services/http_server/server.hpp>

namespace rocketjoe { namespace services {
    http_dispatcher::http_dispatcher(network::server* ptr,goblin_engineer::dynamic_config&) :
                    abstract_service(ptr, "router"){
                    detail::wrapper_router wrapper_router_;
                    /*wrapper_router_.http_get(
                        "/ping",
                        [](network::query_context&request){
                            request.response().body()="pong";
                            request.write();
                        }
                    );
                     */

                    router_ = std::move(wrapper_router_.get_router());

                    add_handler(
                            "dispatcher",
                            [this](actor_zeta::actor::context &ctx,::rocketjoe::network::query_context&context){
                                router_.invoke(context);
                            }
                    );

            }

}}
