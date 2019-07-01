#include <rocketjoe/services/router/service_router.hpp>
#include <rocketjoe/http/http.hpp>

#include <nlohmann/json.hpp>

#include <unordered_map>
#include <unordered_set>
//#include <rocketjoe/dto/json_rpc.hpp>
#include <goblin-engineer/dynamic.hpp>
#include <rocketjoe/dto/json_rpc.hpp>
#include <rocketjoe/http/query_context.hpp>


namespace rocketjoe { namespace services {

    using api::json_rpc::parse;
/*
    class router::impl final {
    public:
        impl() = default;

        ~impl() = default;

                auto is_reg_app(const std::string &app_name) const -> bool {
                    return app_reg.find(app_name) != app_reg.end();
                }

                auto add_registri_app_name(const api::app_info &app_) -> void {
                    app_reg.emplace(app_.name);
                    api_key_to_app_name.emplace(app_.api_key, app_.name);
                    app_id_to_app_name.emplace(app_.application_id, app_.name);
                }

            private:

                ///app_id -> app_name
                std::unordered_map<std::string, std::string> app_id_to_app_name;

                ///api_key-> app_name
                std::unordered_map<std::string, std::string> api_key_to_app_name;

                /// app_name
                std::unordered_set<std::string> app_reg;

            };

    */

        http_dispatcher::http_dispatcher(goblin_engineer::dynamic_config& ,goblin_engineer::abstract_environment * env) :
                    abstract_service(env, "router"){
                    wrapper_router wrapper_router_;
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


/*

                attach(
                        actor_zeta::behavior::make_handler(
                                "dispatcher",
                                [this](actor_zeta::behavior::context &ctx,http::query_context) -> void {
                                    auto& transport = ctx.message().body<api::transport>();
                                    auto *http = static_cast<api::http *>(transport.detach());


                                    if (http->uri() == "/system") {
                                        api::task task_;
                                        task_.transport_= http;
                                        parse(http->body(), task_.request);

                                        ctx->addresses("object_storage")->send(
                                                actor_zeta::messaging::make_message(
                                                        ctx->self(),
                                                        "create-app",
                                                       std::move(task_)
                                                )
                                        );
                                        return ;
                                    }


                                    if (pimpl->is_reg_app(http->method())) {
                                        api::task task_;
                                        task_.transport_= http;
                                        parse(http->body(), task_.request);

                                        ctx->addresses("object_storage")->send(
                                                actor_zeta::messaging::make_message(
                                                        ctx->self(),
                                                        task_.request.method,
                                                        std::move(task_)
                                                )
                                        );
                                        return ;
                                    }


                                    {
                                        ctx->addresses("lua_engine")->send(
                                                actor_zeta::messaging::make_message(
                                                        ctx->self(),
                                                        "dispatcher",
                                                        std::move(api::transport(http))
                                                )
                                        );
                                        return ;
                                    }


                                }
                        )
                );


                attach(
                        actor_zeta::behavior::make_handler(
                                "registered_application",
                                [this](actor_zeta::behavior::context &ctx) -> void {
                                    auto app_info_t = ctx.message().body<api::app_info>();
                                    pimpl->add_registri_app_name(app_info_t);
                                }
                        )
                );
                */

            }

            void http_dispatcher::startup(goblin_engineer::context_t *) {

            }

            void http_dispatcher::shutdown() {

            }

}}
