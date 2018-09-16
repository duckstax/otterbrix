#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace database {

            class database final: public goblin_engineer::abstract_service_unmanaged {
            public:
                database(goblin_engineer::context_t *ctx);

                ~database();

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
