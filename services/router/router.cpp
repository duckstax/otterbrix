#include <rocketjoe/services/router/router.hpp>
#include <rocketjoe/http/http.hpp>
#include <rocketjoe/http/application.hpp>

#include <nlohmann/json.hpp>

#include <unordered_map>
#include <unordered_set>
//#include <rocketjoe/dto/json_rpc.hpp>
#include <goblin-engineer/dynamic.hpp>
#include <rocketjoe/dto/json_rpc.hpp>
#include <rocketjoe/http/router.hpp>


namespace rocketjoe { namespace services {

    using api::json_rpc::parse;

            router::router(goblin_engineer::dynamic_config& ,goblin_engineer::abstract_environment * env) :
                    abstract_service(env, "router"){
                    http::wrapper_router wrapper_router_;
                    wrapper_router_.http_get(
                        "/ping",
                        [](http::query_context&request){
                            request.response().body()="pong";
                            request.write();
                        }
                    );

                    router_ = std::move(wrapper_router_.get_router());

                    attach(
                        actor_zeta::behavior::make_handler(
                                "dispatcher",
                                [this](actor_zeta::behavior::context &ctx,http::query_context&context){
                                    router_.invoke(context);
                                }
                        )
                    );
            }

            void router::startup(goblin_engineer::context_t *) {

            }

            void router::shutdown() {

            }

}}}
