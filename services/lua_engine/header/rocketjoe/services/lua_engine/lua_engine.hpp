#pragma once

#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>
#include <rocketjoe/services/http_server/server.hpp>
#include <goblin-engineer.hpp>

namespace rocketjoe { namespace services {

    class lua_vm final : public goblin_engineer::abstract_service {
    public:
        lua_vm(rocketjoe::network::server* ptr,goblin_engineer::dynamic_config&);

        ~lua_vm() override = default;

    private:
        std::unique_ptr<lua_engine::lua_context> pimpl;
    };

}}
