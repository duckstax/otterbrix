#include "init_service.hpp"

#include <actor-zeta/environment/group.hpp>
#include <actor-zeta/environment/cooperation.hpp>

#include <rocketjoe/services/lua_engine/lua_engine.hpp>
#include <rocketjoe/services/flat_cache/flat_cache.hpp>
#include <rocketjoe/services/router/router.hpp>
#include <rocketjoe/services/object_storage/object_storage.hpp>
#include <rocketjoe/http/http_server.hpp>


void init_service(goblin_engineer::dynamic_environment&env) {

/// rewrite config
        auto& lua = env.add_service<rocketjoe::services::lua_engine::lua_engine>();
/// rewrite config

        auto& object_storage = env.add_service<rocketjoe::services::object_storage::object_storage>();
        auto& cache = env.add_service<rocketjoe::services::flat_cache::flat_cache>();
        auto& router = env.add_service<rocketjoe::services::router::router>();

        //auto& websocket = env.add_data_provider<rocketjoe::data_provider::websocket::websocket>(router->entry_point());
        auto& http = env.add_data_provider<rocketjoe::http::http_server>(router->entry_point());

        router->add_shared(http.address().operator->());
        lua->add_shared(http.address().operator->());
        object_storage->add_shared(http.address().operator->());

        actor_zeta::environment::link(router,object_storage);
        actor_zeta::environment::link(router,lua);
        actor_zeta::environment::link(lua,object_storage);

        router->join(cache);
        lua->join(cache);

}