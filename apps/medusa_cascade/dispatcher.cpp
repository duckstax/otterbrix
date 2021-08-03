#include "dispatcher.hpp"
#include "tracy/tracy.hpp"

namespace kv {

    manager_dispatcher_t::manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput)
        : goblin_engineer::abstract_manager_service("manager_dispatcher")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        ZoneScoped;
        e_->start();
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        e_->stop();
    }

    auto manager_dispatcher_t::executor() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }
    auto manager_dispatcher_t::get_executor() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }

    //NOTE: behold thread-safety!
    auto manager_dispatcher_t::enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
        ZoneScoped;
        set_current_message(std::move(msg));
        execute(*this);
    }

} // namespace kv