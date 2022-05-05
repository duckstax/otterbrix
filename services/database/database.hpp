#pragma once
#include <memory>
#include <unordered_map>

#include <core/excutor.hpp>

#include "log/log.hpp"

#include "forward.hpp"
#include "route.hpp"

namespace services::database {

    class manager_database_t final : public actor_zeta::cooperative_supervisor<manager_database_t> {
    public:
        manager_database_t(actor_zeta::detail::pmr::memory_resource*,log_t& log, size_t num_workers, size_t max_throughput);
        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* ;
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override;
        ~manager_database_t();
        void create(session_id_t& session, std::string& name);

    private:
        log_t log_;
        actor_zeta::scheduler_ptr e_;
        std::unordered_map<std::string, actor_zeta::address_t> databases_;
    };

    using manager_database_ptr = std::unique_ptr<manager_database_t>;

    class database_t final : public actor_zeta::cooperative_supervisor<database_t> {
    public:
        database_t(manager_database_ptr, std::string name, log_t& log, size_t num_workers, size_t max_throughput);
        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* final override;
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override;
        ~database_t();
        void create(session_id_t& session, std::string& name, actor_zeta::address_t mdisk);
        void drop(session_id_t& session, std::string& name);
        const std::string& name();
    private:
        const std::string name_;
        log_t log_;
        actor_zeta::scheduler_ptr e_;
        std::unordered_map<std::string, actor_zeta::actor> collections_;
    };

    using database_ptr = std::unique_ptr<database_t>;

} // namespace services::storage