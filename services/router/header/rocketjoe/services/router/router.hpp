#pragma once

#include <goblin-engineer/abstract_service.hpp>
#include <rocketjoe/http/router.hpp>

namespace rocketjoe { namespace services { namespace router {

            class router final: public goblin_engineer::abstract_service {
            public:
                router(goblin_engineer::dynamic_config&, goblin_engineer::abstract_environment * );

                ~router() override = default;
                
                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;
            private:
                http::router router_;
                ///class impl;
                //std::unique_ptr<impl> pimpl;
            };

}}}