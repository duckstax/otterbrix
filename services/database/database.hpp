#pragma once
#include <memory>
#include <unordered_map>

#include <components/excutor.hpp>
#include <goblin-engineer/core.hpp>

#include "log/log.hpp"

#include "forward.hpp"
#include "route.hpp"

namespace services::storage {

    class manager_database_t final : public goblin_engineer::abstract_manager_service {
    public:
        manager_database_t(log_t& log, size_t num_workers, size_t max_throughput);
        auto executor_impl() noexcept -> goblin_engineer::abstract_executor* final override;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;
        ~manager_database_t();
        void create(session_t& session, std::string& name);

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
        std::unordered_map<goblin_engineer::string_view, goblin_engineer::address_t> databases_;
    };

    using manager_database_ptr = goblin_engineer::intrusive_ptr<manager_database_t>;

    class database_t final : public goblin_engineer::abstract_manager_service {
    public:
        database_t(goblin_engineer::supervisor_t*, std::string name, log_t& log, size_t num_workers, size_t max_throughput);
        auto executor_impl() noexcept -> goblin_engineer::abstract_executor* final override;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;
        ~database_t();
        void create(session_t& session, std::string& name);
        void drop(session_t& session, std::string& name);

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
        std::unordered_map<goblin_engineer::string_view, goblin_engineer::address_t> collections_;
    };

    using database_ptr = goblin_engineer::intrusive_ptr<database_t>;

} // namespace services::storage