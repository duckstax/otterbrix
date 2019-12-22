#pragma  once

#include <array>
#include <map>

#include <boost/filesystem.hpp>

#include <sol.hpp>

#include <goblin-engineer.hpp>

#include <rocketjoe/services/lua_engine/device.hpp>
#include <rocketjoe/network/network.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

    class lua_context final {
    public:

        lua_context(goblin_engineer::dynamic_config&,actor_zeta::actor::actor_address);

        ~lua_context() = default;

        auto push_job(network::query_context &&) -> void;

        auto run() -> void;

    private:
        device<network::query_context,network::request_type ,network::response_type > device_;
        sol::environment environment;
        sol::state lua;
        boost::filesystem::path path_script;
        actor_zeta::actor::actor_address address;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}}