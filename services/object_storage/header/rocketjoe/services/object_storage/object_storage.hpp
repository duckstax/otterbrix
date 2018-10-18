#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace object_storage {

            class object_storage final: public goblin_engineer::abstract_service_unmanaged {
            public:
                object_storage(goblin_engineer::context_t *ctx);

                ~object_storage();

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
