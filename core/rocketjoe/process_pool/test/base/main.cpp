#include "../../process_pool.hpp"

int main(){
    goblin_engineer::dynamic_config cfg;
    goblin_engineer::root_manager app(cfg);
    rocketjoe::services::process_pool_t base(&app,cfg);

    base.add_worker_process();
    return 0;

}