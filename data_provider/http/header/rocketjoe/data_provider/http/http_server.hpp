#pragma once

#include <goblin-engineer/data_provider.hpp>

namespace rocketjoe { namespace data_provider { namespace http_server {

            class http_server final: public goblin_engineer::data_provider {
            public:
                http_server(goblin_engineer::context_t *ctx,actor_zeta::actor::actor_address);

                ~http_server();

                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;

            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

        }
    }
}
