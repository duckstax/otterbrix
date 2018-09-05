#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace RocketJoe {
    namespace services {
        namespace ws_server {

            class ws_server final: public goblin_engineer::abstract_service_unmanaged {
            public:
                ws_server(goblin_engineer::context_t *);

                ~ws_server();

                void metadata(goblin_engineer::metadata_service*) const override;

                std::string name() const override;

                void startup(goblin_engineer::context_t *ctx) override;

                void shutdown() override;

            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

        }
    }
}
