
#include <goblin-engineer.hpp>
#include "control_center.hpp"


namespace rocketjoe { namespace network {

        control_center::control_center(
                goblin_engineer::root_manager *env,
                goblin_engineer::dynamic_config &
        ): network_manager_service(env, "timer_manager",1){




        }
}}