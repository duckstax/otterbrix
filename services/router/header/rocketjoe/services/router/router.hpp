#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace router {

            class router final: public goblin_engineer::abstract_service {
            public:
                router(goblin_engineer::dynamic_config&, goblin_engineer::abstract_environment * );

                ~router();
                
                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;
            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

}}}