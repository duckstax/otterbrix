#include "init_service.hpp"

#include <rocketjoe/services/python_sandbox/python_sandbox.hpp>
#include <rocketjoe/services/router/service_router.hpp>
#include <rocketjoe/services/http_server/server.hpp>

void init_service(goblin_engineer::root_manager& env,goblin_engineer::dynamic_config&cfg) {

        auto* http = env.add_manager_service<rocketjoe::network::server>();

        auto python = goblin_engineer::make_service<rocketjoe::services::python_sandbox_t>(http, cfg);
        auto router = goblin_engineer::make_service<rocketjoe::services::http_dispatcher>(http,cfg);

        actor_zeta::link(http,router);
        actor_zeta::link(http,python);

}
