#pragma once

#include <goblin-engineer/abstract_service.hpp>


namespace rocketjoe { namespace services {

            class client final: public goblin_engineer::abstract_service {
            public:
                client(goblin_engineer::dynamic_config&, goblin_engineer::abstract_environment * );

                ~client() override = default;
                
                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;
            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

}}