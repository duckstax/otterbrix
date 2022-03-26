#include "manager_wal_replicate.hpp"
#include "wal.hpp"

manager_wal_replicate_t::manager_wal_replicate_t(boost::filesystem::path path,log_t& log, size_t num_workers, size_t max_throughput)
    : manager("manager_wal")
    , path_(path)
    ,log_(log.clone())
    , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
    trace(log_,"manager_wal_replicate_t num_workers : {} , max_throughput: {}", num_workers, max_throughput);
    add_handler("create",&manager_wal_replicate_t::creat_wal_worker);
    add_handler("insert_many",&manager_wal_replicate_t::insert_many);
    trace(log_,"manager_wal_replicate_t start thread pool");
    e_->start();
}

auto manager_wal_replicate_t::executor_impl() noexcept -> goblin_engineer::abstract_executor* {
    return e_.get();
}

auto manager_wal_replicate_t::enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
    set_current_message(std::move(msg));
    execute();
}

auto manager_wal_replicate_t::add_actor_impl(goblin_engineer::actor a) -> void {
    actor_storage_.emplace_back(std::move(a));
}

auto manager_wal_replicate_t::add_supervisor_impl(goblin_engineer::supervisor) -> void {
    log_.error("manager_dispatcher_t::add_supervisor_impl");
}

void manager_wal_replicate_t::creat_wal_worker() {
    auto address = spawn_actor<wal_replicate_t>(log_,path_);
    dispathers_.emplace_back(address);
}

void manager_wal_replicate_t::insert_many(insert_many_t& data) {
    trace(log_,"manager_wal_replicate_t::insert_many");
    goblin_engineer::send(dispathers_[0],address(),"insert_many",std::move(data));
}
