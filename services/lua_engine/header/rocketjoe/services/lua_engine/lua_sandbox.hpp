#pragma  once

#include <array>
#include <map>

#include <sol.hpp>
#include <goblin-engineer/abstract_service.hpp>

#include <rocketjoe/services/lua_engine/device.hpp>
#include <rocketjoe/http/http.hpp>

namespace rocketjoe { namespace services { namespace detail {

    class lua_context final {
    public:

        lua_context(goblin_engineer::dynamic_config&,actor_zeta::actor::actor_address);

        ~lua_context() = default;

        auto push_job(http::query_context &&) -> void;

        auto run() -> void;

    private:
        device<http::query_context,http::request_type ,http::response_type > device_;
        sol::environment environment;
        sol::state lua;
        std::string path_script;
        actor_zeta::actor::actor_address address;
        std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
    };

}}}