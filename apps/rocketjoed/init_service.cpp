#include "init_service.hpp"

#include <actor-zeta/environment/cooperation.hpp>

#include <rocketjoe/services/python_engine/python_engine.hpp>
#include <rocketjoe/services/router/service_router.hpp>
#include <rocketjoe/services/http_server/server.hpp>

void init_service(goblin_engineer::dynamic_environment&env) {

/// rewrite config
//      auto& lua = env.add_service<rocketjoe::services::lua_engine::lua_engine>();
        auto& python = env.add_service<rocketjoe::services::python_vm>();
/// rewrite config


        auto& router = env.add_service<rocketjoe::services::http_dispatcher>();
        auto& http = env.add_data_provider<rocketjoe::network::server>(router->entry_point());

        router->add_shared(http.address().operator->());
//      lua->add_shared(http.address().operator->());
        python->add_shared(http.address().operator->());
//      object_storage->add_shared(http.address().operator->());

//      actor_zeta::environment::link(router,lua);
        actor_zeta::environment::link(router,python);
}
