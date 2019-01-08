#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace object_storage {

            class object_storage final: public goblin_engineer::abstract_service {
            public:
                object_storage(goblin_engineer::dynamic_config&, goblin_engineer::abstract_environment *);

                ~object_storage() = default;

                auto startup(goblin_engineer::context_t *) -> void override;

                auto shutdown() -> void override;
            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

        }
    }
}
