#pragma once

#include <unordered_map>

#include <goblin-engineer.hpp>
#include <goblin-engineer/components/network.hpp>

namespace rocketjoe { namespace network {

    boost::asio::local:
    class control_center final : public goblin_engineer::components::network_manager_service {
    public:
        control_center(
                goblin_engineer::root_manager *,
                goblin_engineer::dynamic_config &
        );


        void write(){

        };

        ~control_center() override = default;

    private:
        std::unordered_map<pid_t,int> d_;

    };

}}
