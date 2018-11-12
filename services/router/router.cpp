#include <rocketjoe/services/router/router.hpp>
#include <rocketjoe/api/transport_base.hpp>
#include <rocketjoe/api/json_rpc.hpp>
#include <rocketjoe/api/http.hpp>

#include <rocketjoe/api/application.hpp>

#include <nlohmann/json.hpp>

#include <unordered_map>
#include <unordered_set>
#include <api/json_rpc.hpp>


namespace rocketjoe {
    namespace services {
        namespace router {

    using api::json_rpc::parse;

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

            router::router(goblin_engineer::context_t *ctx) :
                    abstract_service(ctx, "router"),
                    pimpl(std::make_unique<impl>()) {


                attach(
                        actor_zeta::behavior::make_handler(
                                "dispatcher",
                                [this](actor_zeta::behavior::context &ctx) -> void {
                                    auto& transport = ctx.message().body<api::transport>();
                                    auto *http = static_cast<api::http *>(transport.detach());


                                    api::task task_;
                                    task_.transport_= http;
                                    parse(http->body(), task_.request);

                                    if (http->uri() == "/system") {
                                        ctx->addresses("object_storage")->send(
                                                actor_zeta::messaging::make_message(
                                                        ctx->self(),
                                                        "create-app",
                                                       std::move(task_)
                                                )
                                        );
                                        return ;
                                    }


                                    if (pimpl->is_reg_app(task_.request.method)) {
                                        ctx->addresses("object_storage")->send(
                                                actor_zeta::messaging::make_message(
                                                        ctx->self(),
                                                        task_.request.method,
                                                        std::move(task_)
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
                                    auto app_info_t = std::move(ctx.message().body<api::app_info>());
                                    pimpl->add_registri_app_name(app_info_t);
                                }
                        )
                );

            }

            router::~router() = default;

            void router::startup(goblin_engineer::context_t *) {

            }

            void router::shutdown() {

            }

}}}
