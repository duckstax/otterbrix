#pragma once

#include <goblin-engineer.hpp>
#include <goblin-engineer/components/network.hpp>

namespace rocketjoe { namespace network {

class server final: public goblin_engineer::components::network_manager_service {
    public:
        server(
                goblin_engineer::dynamic_config &,
                goblin_engineer::dynamic_environment *
        );

        ~server() override = default;

    private:
        class impl;
        std::shared_ptr<impl> pimpl;
    };

}}
