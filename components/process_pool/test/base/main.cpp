#include <components/process_pool/process_pool.hpp>

int main(){
    auto log = components::initialization_logger();
    components::process_pool_t process_pool("debug-worker",{"-f"},log);
    process_pool.add_worker_process();
    return 0;

}
