#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace http_server {

            class http_server final: public goblin_engineer::abstract_service_unmanaged {
            public:
                http_server(goblin_engineer::context_t *ctx);

                ~http_server();

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
