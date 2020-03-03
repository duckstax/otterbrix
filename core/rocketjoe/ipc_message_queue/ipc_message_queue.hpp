#pragma once

#include <unordered_map>

#include <goblin-engineer.hpp>
#include <goblin-engineer/components/network.hpp>

#include "detail/service/message_queue.hpp"

namespace rocketjoe { namespace network {

    class ipc_message_queue final : public goblin_engineer::components::network_manager_service {
    public:
        ipc_message_queue(
                goblin_engineer::root_manager *,
                goblin_engineer::dynamic_config &
        );


        void write(){

        };

        ~ipc_message_queue() override = default;

    private:
        services::message_queue  ipc_mq_;
    };

}}
