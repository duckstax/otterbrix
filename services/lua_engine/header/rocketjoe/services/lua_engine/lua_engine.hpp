#pragma once

#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>
#include <goblin-engineer.hpp>

namespace rocketjoe { namespace services {

    class lua_vm final : public goblin_engineer::abstract_service {
    public:
        lua_vm(goblin_engineer::dynamic_config &, goblin_engineer::abstract_environment *);

        ~lua_vm() override = default;

    private:
        std::unique_ptr<lua_engine::lua_context> pimpl;
    };

}}
