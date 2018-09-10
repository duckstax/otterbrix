#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace RocketJoe { namespace services { namespace router {

            class router final: public goblin_engineer::abstract_service_unmanaged {
            public:
                router(goblin_engineer::context_t *ctx);

                ~router();

                auto  metadata(goblin_engineer::metadata_service*) const -> void override;

                auto name() const -> std::string override;

                auto startup(goblin_engineer::context_t *) -> void override;

                auto shutdown() -> void override;
            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

        }
    }
}
