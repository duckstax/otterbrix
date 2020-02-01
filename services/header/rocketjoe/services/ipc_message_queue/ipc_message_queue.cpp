
#include <goblin-engineer.hpp>
#include "ipc_message_queue.hpp"


namespace rocketjoe { namespace network {

    ipc_message_queue::ipc_message_queue(
                goblin_engineer::root_manager *env,
                goblin_engineer::dynamic_config & cfg
        )
        : network_manager_service(env, "timer_manager",1)
        , ipc_mq_(loop(),cfg.as_object()["ipc_message_queue_id"].as_string()) {




        }
}}