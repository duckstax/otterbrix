#pragma once

#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace flat_cache {

            class flat_cache_wrapper final: public goblin_engineer::abstract_service {
            public:
                flat_cache_wrapper(goblin_engineer::context_t *ctx);

                ~flat_cache_wrapper() = default;

                void startup(goblin_engineer::context_t *) override;

                void shutdown() override;
            private:
                class impl;
                std::unique_ptr<impl> pimpl;
            };

            using flat_cache_service = flat_cache_wrapper;


}}}