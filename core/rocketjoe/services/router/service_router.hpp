#pragma once

#include <goblin-engineer.hpp>
#include <rocketjoe/services/router/router.hpp>
#include <rocketjoe/services/http_server/server.hpp>

namespace rocketjoe { namespace services {

        class http_dispatcher final : public goblin_engineer::abstract_service {
        public:
            http_dispatcher(network::server *,goblin_engineer::dynamic_config&);

            ~http_dispatcher() override = default;

        private:
            detail::router router_;
        };
}}