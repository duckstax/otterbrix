#include "init_service.hpp"

#include <rocketjoe/python_sandbox/python_sandbox.hpp>

#include <goblin-engineer/components/root_manager.hpp>

using goblin_engineer::components::make_manager_service;
using goblin_engineer::components::make_service;
using goblin_engineer::components::root_manager;

using actor_zeta::link;

constexpr const static bool master = true;

void init_service(goblin_engineer::components::root_manager& env, nlohmann::json &cfg) {
    auto python= make_manager_service<rocketjoe::services::python_sandbox_t>(env,cfg);
    python->init();
    python->start();


    // TODO: fix
    /*
    if (cfg.as_object()["master"].as_bool() == master) {
        auto* http = env.add_manager_service<rocketjoe::network::server>();
        auto router = make_service<rocketjoe::network::http_dispatcher>(http, cfg);
        link(http, router);
    }
     */
}
