#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace RocketJoe { namespace services { namespace lua_engine {

            class lua_engine final: public goblin_engineer::abstract_service_unmanaged {
            public:
                lua_engine(goblin_engineer::context_t *);

                ~lua_engine();

                void metadata(goblin_engineer::metadata_service*) const override;

                std::string name() const override;

                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;

            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

        }
    }
}
