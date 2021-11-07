#pragma once

#include <unordered_map>

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>

#include "forward.hpp"
#include <RocketJoe/components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/excutor.hpp>
#include <services/storage/result.hpp>
#include <services/storage/result_insert_one.hpp>

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
    void create_database(duck_charmer::session_t& session, std::string& name, std::function<void(goblin_engineer::actor_address)>& callback);
    void create_database_finish(duck_charmer::session_t& session, goblin_engineer::actor_address address);
    void create_collection(duck_charmer::session_t& session, std::string& name, std::function<void(goblin_engineer::actor_address)>& callback);
    void create_collection_finish(duck_charmer::session_t& session, goblin_engineer::actor_address address);
    void insert(duck_charmer::session_t& session,std::string& collection,components::document::document_t& document, std::function<void(result_insert_one&)>& callback);
    void insert_finish(duck_charmer::session_t& session,result_insert_one&result);
    void find(duck_charmer::session_t& session, std::string& collection, components::document::document_t &condition, std::function<void(duck_charmer::session_t& session,components::cursor::cursor_t*)>& callback);
    void find_finish(duck_charmer::session_t&, components::cursor::sub_cursor_t*result);
    void size(duck_charmer::session_t& session, std::string& collection, std::function<void (result_size &)> &callback);
    void size_finish(duck_charmer::session_t&, result_size &result);

    void close_cursor(duck_charmer::session_t& session);
    void has_next_cursor(duck_charmer::session_t& session, std::function<void (std::size_t)> &callback);//todo
    void next_cursor(duck_charmer::session_t& session, std::function<void (std::size_t)> &callback);//todo
    void size_cursor(duck_charmer::session_t& session, std::function<void (std::size_t)> &callback);
    void get_cursor(duck_charmer::session_t& session, std::function<void (std::size_t)> &callback);//todo
    void print_cursor(duck_charmer::session_t& session, std::function<void (const std::string &)> &callback);

private:
    std::function<void(goblin_engineer::actor_address)> create_database_and_collection_callback_;
    std::function<void(result_insert_one&)> insert_callback_;
    std::function<void(duck_charmer::session_t&,components::cursor::cursor_t*)> find_callback_;
    std::function<void(result_size&)> size_callback_;
    log_t log_;
    std::unordered_map<duck_charmer::session_t,std::unique_ptr<components::cursor::cursor_t>> cursor_;
    std::unordered_map<std::string,goblin_engineer::actor_address> collection_address_book_;
};
