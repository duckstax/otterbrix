#include "manager_wal_replicate.hpp"


manager_wal_replicate_t::manager_wal_replicate_t(log_t& log, size_t num_workers, size_t max_throughput)
    : manager("manager_dispatcher")
    , log_(log.clone())
    , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
    log_.trace("manager_dispatcher_t::manager_dispatcher_t num_workers : {} , max_throughput: {}", num_workers, max_throughput);

    log_.trace("manager_dispatcher_t start thread pool");
    e_->start();
}
