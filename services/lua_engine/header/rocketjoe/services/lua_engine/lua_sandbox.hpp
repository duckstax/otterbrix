#pragma  once

#include <array>
#include <map>

#include <rocketjoe/services/lua_engine/device.hpp>
#include <rocketjoe/api/transport_base.hpp>
#include <sol.hpp>
#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

    class lua_context final {
    public:

        lua_context(goblin_engineer::dynamic_config&,actor_zeta::actor::actor_address);

        ~lua_context() = default;

        auto push_job(api::transport &&) -> void;

        auto run() -> void;

    private:
        device<api::transport> device_;
        sol::environment environment;
        sol::state lua;
        std::string path_script;
        actor_zeta::actor::actor_address address;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}}