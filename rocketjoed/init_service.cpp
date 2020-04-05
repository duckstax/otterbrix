#include "init_service.hpp"

#include <rocketjoe/network/server.hpp>
#include <rocketjoe/network/service_router.hpp>
#include <rocketjoe/python_sandbox/python_sandbox.hpp>

using goblin_engineer::dynamic_config;
using goblin_engineer::make_service;
using goblin_engineer::root_manager;

using actor_zeta::link;

constexpr const static bool master = true;

void init_service(root_manager& env, dynamic_config& cfg) {
    auto* python_env = env.add_manager_service<rocketjoe::services::python_sandbox_t>();
    python_env->init();
    python_env->start();

    if (cfg.as_object()["master"].as_bool() == master) {
        auto* http = env.add_manager_service<rocketjoe::network::server>();
        auto router = make_service<rocketjoe::network::http_dispatcher>(http, cfg);
        link(http, router);
    }
}
