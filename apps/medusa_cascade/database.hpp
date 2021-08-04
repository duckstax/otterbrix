#pragma once
#include <memory>
#include <unordered_map>

#include <goblin-engineer/core.hpp>

#include "log/log.hpp"

#include "forward.hpp"
#include "protocol.hpp"
#include "route.hpp"

namespace kv {

    class manager_database_t final : public goblin_engineer::abstract_manager_service {
    public:
        manager_database_t(log_t& log, size_t num_workers, size_t max_throughput);

        auto executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto get_executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;
        ~manager_database_t();
        void create() {
        }

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
    };

    using manager_database_ptr = goblin_engineer::intrusive_ptr<manager_database_t>;

    class database_t final : public goblin_engineer::abstract_manager_service {
    public:
        database_t(manager_database_ptr supervisor, log_t& log, size_t num_workers, size_t max_throughput);
        auto executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto get_executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;
        ~database_t();
        void collection() {
        }

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
    };

    using database_ptr = goblin_engineer::intrusive_ptr<database_t>;

} // namespace kv