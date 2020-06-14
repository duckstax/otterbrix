#include "init_service.hpp"

#include <goblin-engineer/components/http.hpp>
#include <goblin-engineer/components/root_manager.hpp>
#include <iostream>
#include <services/interactive_python_interpreter/interactive_python_interpreter.hpp>

using goblin_engineer::components::make_manager_service;
using goblin_engineer::components::make_service;
using goblin_engineer::components::root_manager;
using namespace goblin_engineer::components;

void init_service(goblin_engineer::components::root_manager& env, components::configuration& cfg, components::log_t& log) {
    if (cfg.python_configuration_.mode_ != components::sandbox_mode_t::script) {
        auto python = make_manager_service<services::interactive_python_interpreter>(env, cfg.python_configuration_, log);
        python->init();
        python->start();
    }

    if (
        (cfg.operating_mode_ == components::operating_mode::master)
        &&
        (cfg.python_configuration_.mode_ != components::sandbox_mode_t::script)) {
        http::router router_;

        router_.http_get(
            R"(/my/super/url)", [](http::query_context& ctx) {
                std::cerr << ctx.response().body().c_str() << std::endl;
                ctx.response().body() = ctx.request().body();
                ctx.write();
            });

        auto http = make_manager_service<http::server>(env, cfg.port_http_);
        make_service<http::http_dispatcher>(http, router_);
    }
}
