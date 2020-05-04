#include <rocketjoe/process_pool/process_pool.hpp>

int main(){
    auto log = rocketjoe::initialization_logger();
    rocketjoe::process_pool_t process_pool("debug_worker",{"-f"},log);
    process_pool.add_worker_process();
    return 0;

}