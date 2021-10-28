#pragma once

#include <unordered_map>

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>

#include "forward.hpp"
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/excutor.hpp>
#include <services/storage/result.hpp>
#include <services/storage/result_insert_one.hpp>

using manager =  goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

class  manager_dispatcher_t final : public manager {
public:
    manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput);
    ~manager_dispatcher_t() override;

protected:
    auto executor_impl() noexcept -> goblin_engineer::abstract_executor* final ;
    auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;
    auto add_actor_impl(goblin_engineer::actor a) -> void override;
    auto add_supervisor_impl(goblin_engineer::supervisor) -> void override;

private:
    log_t log_;
    goblin_engineer::executor_ptr e_;
    std::vector<goblin_engineer::actor> actor_storage_;
    std::unordered_map<std::string,goblin_engineer::address_t> dispatcher_to_address_book_;
};

using manager_dispatcher_ptr = goblin_engineer::intrusive_ptr<manager_dispatcher_t>;

class  dispatcher_t final : public goblin_engineer::abstract_service {
public:
    dispatcher_t(goblin_engineer::supervisor_t* manager_database, log_t& log);
    void create_database(duck_charmer::session_t& session, std::string& name, std::function<void(goblin_engineer::address_t)>& callback);
    void create_database_finish(duck_charmer::session_t& session, goblin_engineer::address_t address);
    void create_collection(duck_charmer::session_t& session, std::string& name, std::function<void(goblin_engineer::address_t)>& callback);
    void create_collection_finish(duck_charmer::session_t& session, goblin_engineer::address_t address);
    void insert(duck_charmer::session_t& session,std::string& collection,components::storage::document_t& document, std::function<void(result_insert_one&)>& callback);
    void insert_finish(duck_charmer::session_t& session,result_insert_one&result);
    void find(duck_charmer::session_t& session, std::string& collection, components::storage::document_t &condition, std::function<void(duck_charmer::session_t& session,components::cursor::cursor_t*)>& callback);
    void find_finish(duck_charmer::session_t&, components::cursor::sub_cursor_t*result);
    void size(duck_charmer::session_t& session, std::string& collection, std::function<void (result_size &)> &callback);
    void size_finish(duck_charmer::session_t&, result_size &result);
    void close_cursor(duck_charmer::session_t& session);

private:
    std::function<void(goblin_engineer::address_t)> create_database_and_collection_callback_;
    std::function<void(result_insert_one&)> insert_callback_;
    std::function<void(duck_charmer::session_t&,components::cursor::cursor_t*)> find_callback_;
    std::function<void(result_size&)> size_callback_;
    log_t log_;
    std::unordered_map<duck_charmer::session_t,std::unique_ptr<components::cursor::cursor_t>> cursor_;
    std::unordered_map<std::string,goblin_engineer::address_t> collection_address_book_;
    std::unordered_map<std::string,goblin_engineer::address_t> database_address_book_;
};
