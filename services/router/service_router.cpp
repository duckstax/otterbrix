#include <rocketjoe/services/router/service_router.hpp>


#include <unordered_map>
#include <unordered_set>

#include <nlohmann/json.hpp>
#include <goblin-engineer/dynamic.hpp>
#include <rocketjoe/dto/json_rpc.hpp>
#include <rocketjoe/services/router/router.hpp>


namespace rocketjoe { namespace services {

    using api::json_rpc::parse;

        http_dispatcher::http_dispatcher(goblin_engineer::dynamic_config& ,goblin_engineer::abstract_environment * env) :
                    abstract_service(env, "router"){
                    detail::wrapper_router wrapper_router_;
                    wrapper_router_.http_get(
                        "/ping",
                        [](network::query_context&request){
                            request.response().body()="pong";
                            request.write();
                        }
                    );

                    router_ = std::move(wrapper_router_.get_router());

                    add_handler(
                            "dispatcher",
                            [this](actor_zeta::actor::context &ctx,network::query_context&context){
                                router_.invoke(context);
                            }
                    );

            }

            void http_dispatcher::startup(goblin_engineer::context_t *) {

            }

            void http_dispatcher::shutdown() {

            }

}}
