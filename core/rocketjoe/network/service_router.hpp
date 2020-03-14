#pragma once

#include <goblin-engineer.hpp>
#include <rocketjoe/network/router.hpp>
#include <rocketjoe/network/server.hpp>

namespace rocketjoe { namespace services {

        class http_dispatcher final : public goblin_engineer::abstract_service {
        public:
            http_dispatcher(network::server *,goblin_engineer::dynamic_config&);

            ~http_dispatcher() override = default;

        private:
            detail::router router_;
        };
}}