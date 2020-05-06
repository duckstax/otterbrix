#include "init_service.hpp"

#include <rocketjoe/python_sandbox/python_sandbox.hpp>

#include <goblin-engineer/components/http.hpp>
#include <goblin-engineer/components/root_manager.hpp>
#include <iostream>

using goblin_engineer::components::make_manager_service;
using goblin_engineer::components::make_service;
using goblin_engineer::components::root_manager;
using namespace goblin_engineer::components;

using actor_zeta::link;

constexpr const static bool master = true;

void init_service(goblin_engineer::components::root_manager& env, nlohmann::json& cfg) {
    auto python = make_manager_service<rocketjoe::services::python_sandbox_t>(env, cfg);
    python->init();
    python->start();

    if (cfg["master"].get<bool>() == master) {
        http::router router_;

        router_.http_get(
            R"(/my/super/url)", [](http::query_context& ctx) {
                std::cerr << ctx.response().body().c_str() << std::endl;
                ctx.response().body() = ctx.request().body();
                ctx.write();
            });

        auto http = make_manager_service<http::server>(env, 9999);
        make_service<http::http_dispatcher>(http, router_);
    }
}
