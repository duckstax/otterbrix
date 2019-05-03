#pragma once

#include <goblin-engineer/data_provider.hpp>

namespace rocketjoe { namespace http {

    class http_server final: public goblin_engineer::data_provider {
    public:
        http_server(
                goblin_engineer::dynamic_config &,
                actor_zeta::environment::abstract_environment *,
                actor_zeta::actor::actor_address
        );

        ~http_server() override = default;

        void startup(goblin_engineer::context_t *) override;

        void shutdown() override;

    private:
        class impl;
        std::shared_ptr<impl> pimpl;
    };

}}
