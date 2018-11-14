#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

            class lua_engine final: public goblin_engineer::abstract_service {
            public:
                explicit lua_engine(goblin_engineer::context_t *);

                ~lua_engine() override = default ;

                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;

            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

        }
    }
}
