#pragma once

#include <goblin-engineer/data_provider.hpp>

namespace rocketjoe {namespace data_provider {namespace ws_server {

            class ws_server final: public goblin_engineer::data_provider {
            public:
                ws_server(goblin_engineer::context_t *,actor_zeta::actor::actor_address  );

                ~ws_server();

                void startup(goblin_engineer::context_t *ctx) override;

                void shutdown() override;

            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

        }
    }
}
