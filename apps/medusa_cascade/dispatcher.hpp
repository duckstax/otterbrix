#pragma once
#include "forward.hpp"
#include "route.hpp"
#include <goblin-engineer/core.hpp>
#include <log/log.hpp>

namespace kv {
    class manager_dispatcher_t final : public goblin_engineer::abstract_manager_service {
    public:
        manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput);
        ~manager_dispatcher_t() override;

        auto executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto get_executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
    };

    using manager_dispatcher_ptr = goblin_engineer::intrusive_ptr<manager_dispatcher_t>;

    class dispatcher_t final : public goblin_engineer::abstract_service {
    public:
        dispatcher_t(manager_dispatcher_ptr manager_database, log_t& log)
            : goblin_engineer::abstract_service(manager_database, "dispatcher")
            , log_(log.clone()) {
            add_handler(dispatcher::create_collection, &dispatcher_t::create_collection);
            add_handler(dispatcher::create_database, &dispatcher_t::create_database);
            add_handler(dispatcher::select, &dispatcher_t::select);
            add_handler(dispatcher::insert, &dispatcher_t::insert);
            add_handler(dispatcher::erase, &dispatcher_t::erase);
        }

        void create_database() {
        }

        void create_collection() {
        }

        void select(session_id_t session_id) {
        }

        void insert(session_id_t session_id) {
        }

        void erase(session_id_t session_id) {
        }

    private:
        log_t log_;
    };
} // namespace kv