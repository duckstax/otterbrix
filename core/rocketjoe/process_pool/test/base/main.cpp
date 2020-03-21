#include <rocketjoe/process_pool/process_pool.hpp>

int main(){
    auto log = rocketjoe::initialization_logger();
    rocketjoe::process_pool_t base(log);
    base.add_worker_process();
    return 0;

}