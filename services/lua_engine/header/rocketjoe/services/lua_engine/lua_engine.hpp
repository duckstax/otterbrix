#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

    class lua_context;

    class lua_engine final : public goblin_engineer::abstract_service {
    public:
        lua_engine(goblin_engineer::dynamic_config &, goblin_engineer::abstract_environment *);

        ~lua_engine() override = default;

        void startup(goblin_engineer::context_t *) override;

        void shutdown() override;

    private:
        std::unique_ptr<lua_context> pimpl;
    };

}}}
