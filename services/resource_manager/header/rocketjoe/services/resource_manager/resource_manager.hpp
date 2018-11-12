#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace resource_manager {


            class resource_manager final: public goblin_engineer::abstract_service{
            public:
                resource_manager(goblin_engineer::context_t *ctx);

                ~resource_manager();

                auto startup(goblin_engineer::context_t *) -> void override;

                auto shutdown() -> void override;
            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

        }
    }
}
