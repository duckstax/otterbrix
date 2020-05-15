
#include <goblin-engineer.hpp>
#include "timer_manager.hpp"


namespace rocketjoe { namespace network {

        timer_manager::timer_manager(
                goblin_engineer::root_manager *env,
                goblin_engineer::dynamic_config &
        )
        : network_manager_service(env, "timer_manager",1)
        , counter_timer_(0){



        }

        void timer_manager::cancel(timer_id id) {
            auto& timer = timer_manager_storage_.at(id);
            timer.cancel();
        }

    }}