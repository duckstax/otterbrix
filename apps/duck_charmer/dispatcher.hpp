#pragma once

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>

#include "storage/forward.hpp"

using services::storage::session_t;

class  manager_dispatcher_t final : public goblin_engineer::abstract_manager_service {
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

class  dispatcher_t final : public goblin_engineer::abstract_service {
public:
    dispatcher_t(manager_dispatcher_ptr manager_database, log_t& log);

    void create_database(session_t& session, std::string& name, std::function<void(goblin_engineer::actor_address)>& callback) {
        log_.debug("create_database_init: {}", name);
        create_database_callback_ = std::move(callback);
        goblin_engineer::send(addresses("manager_database"), self(), "create_database",session, name);
    }

    void create_database_finish(session_t& session, goblin_engineer::actor_address address) {
        log_.debug("create_database_finish: {}", address->type());
        create_database_callback_(address);
    }

    void create_collection(session_t& session, std::string& name, std::function<void(goblin_engineer::actor_address)>& callback) {
        log_.debug("create_database_init: {}", name);
        create_database_callback_ = std::move(callback);
        goblin_engineer::send(addresses("manager_database"), self(), "create_database",session, name);
    }

    void create_collection_finish(session_t& session, goblin_engineer::actor_address address) {
        log_.debug("create_database_finish: {}", address->type());
        create_database_callback_(address);
    }

/*
    void select(session_t session, const select_t& value);

    void insert(session_t session, const insert_t& value);

    void erase(session_t session, const erase_t& value);
    */

private:
    std::function<void(goblin_engineer::actor_address)> create_database_callback_;
    log_t log_;
    ///boost::uuids::string_generator string_generator_;
};