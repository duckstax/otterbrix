#pragma once

#include <unordered_map>

#include <goblin-engineer/core.hpp>
#include <components/log/log.hpp>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/excutor.hpp>
#include <components/session/session.hpp>

#include <services/storage/result.hpp>
#include <services/storage/result_insert_one.hpp>
#include "storage/result_database.hpp"

namespace services::dispatcher {

    using manager = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

    class manager_dispatcher_t final : public manager {
    public:
        manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput);
        ~manager_dispatcher_t() override;
        void create( components::session::session_t& session,std::string& name );
        void connect_me(components::session::session_t& session, std::string& name);
        void create_database(components::session::session_t& session, std::string& name);
        void create_collection(components::session::session_t& session, std::string& database_name, std::string& collection_name);
        void insert(components::session::session_t& session, std::string& collection, components::document::document_t& document);
        void find(components::session::session_t& session, std::string& collection, components::document::document_t& condition);
        void size(components::session::session_t& session, std::string& collection);
        void close_cursor(components::session::session_t& session);
    protected:
        auto executor_impl() noexcept -> goblin_engineer::abstract_executor* final;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;
        auto add_actor_impl(goblin_engineer::actor a) -> void override;
        auto add_supervisor_impl(goblin_engineer::supervisor) -> void override;

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
        std::vector<goblin_engineer::actor> actor_storage_;
        std::unordered_map<std::string, goblin_engineer::address_t> dispatcher_to_address_book_;
        std::vector<goblin_engineer::address_t> dispathers_;
    };

    using manager_dispatcher_ptr = goblin_engineer::intrusive_ptr<manager_dispatcher_t>;

    class dispatcher_t final : public goblin_engineer::abstract_service {
    public:
        dispatcher_t(goblin_engineer::supervisor_t* manager_database,goblin_engineer::address_t, log_t& log,std::string name);
        void create_database(components::session::session_t& session, std::string& name,goblin_engineer::address_t address);
        void create_database_finish(components::session::session_t& session,storage::database_create_result,goblin_engineer::address_t);
        void create_collection(components::session::session_t& session, std::string& database_name,std::string& collections_name,goblin_engineer::address_t address);
        void create_collection_finish(components::session::session_t& session,storage::collection_create_result,goblin_engineer::address_t);
        void insert(components::session::session_t& session, std::string& collection, components::document::document_t& document,goblin_engineer::address_t address);
        void insert_finish(components::session::session_t& session, result_insert_one& result);
        void find(components::session::session_t& session, std::string& collection, components::document::document_t& condition);
        void find_finish(components::session::session_t&, components::cursor::sub_cursor_t* result);
        void size(components::session::session_t& session, std::string& collection, goblin_engineer::address_t address);
        void size_finish(components::session::session_t&, result_size& result);
        void close_cursor(components::session::session_t& session);

    private:
        log_t log_;
        goblin_engineer::address_t mdb_;
        std::unordered_map<components::session::session_t,goblin_engineer::address_t > session_to_address_;
        std::unordered_map<components::session::session_t, std::unique_ptr<components::cursor::cursor_t>> cursor_;
        std::unordered_map<std::string, goblin_engineer::address_t> collection_address_book_;
        std::unordered_map<std::string, goblin_engineer::address_t> database_address_book_;
    };

}