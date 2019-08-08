#pragma once

#include <goblin-engineer/abstract_service.hpp>
#include <rocketjoe/services/router/router.hpp>

namespace rocketjoe { namespace services {

            class http_dispatcher final: public goblin_engineer::abstract_service {
            public:
                http_dispatcher(goblin_engineer::dynamic_config&, goblin_engineer::abstract_environment * );

                ~http_dispatcher() override = default;
                
                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;
            private:

                detail::router router_;
                ///class impl;
                //std::unique_ptr<impl> pimpl;
            };

}}