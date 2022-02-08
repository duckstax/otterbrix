#pragma once

#include <unordered_map>

#include <goblin-engineer/core.hpp>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/excutor.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>

#include "services/database/result_database.hpp"
#include <services/collection/result.hpp>

namespace services::dispatcher {

    using manager = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

    class manager_dispatcher_t final : public manager {
    public:
        manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput);
        ~manager_dispatcher_t() override;
        void create(components::session::session_t& session, std::string& name);
        void connect_me(components::session::session_t& session, std::string& name);
        void create_database(components::session::session_t& session, std::string& name);
        void create_collection(components::session::session_t& session, std::string& database_name, std::string& collection_name);
        void drop_collection(components::session::session_t& session, std::string& database_name, std::string& collection_name);
        void insert_one(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& document);
        void insert_many(components::session::session_t& session, std::string& database_name, std::string& collection, std::list<components::document::document_t>& documents);
        void find(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition);
        void find_one(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition);
        void delete_one(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition);
        void delete_many(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition);
        void update_one(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition, components::document::document_t update, bool upsert);
        void update_many(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition, components::document::document_t update, bool upsert);
        void size(components::session::session_t& session, std::string& database_name, std::string& collection);
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

    class key_collection_t {
    public:
        key_collection_t(const std::string& database, const std::string& collection);
        key_collection_t() = delete;
        const std::string& database() const;
        const std::string& collection() const;
        bool operator==(const key_collection_t& other) const;

        struct hash {
            std::size_t operator()(const key_collection_t& key) const;
        };

    private:
        const std::string database_;
        const std::string collection_;
    };

    class dispatcher_t final : public goblin_engineer::abstract_service {
    public:
        dispatcher_t(goblin_engineer::supervisor_t* manager_database, goblin_engineer::address_t, log_t& log, std::string name);
        void create_database(components::session::session_t& session, std::string& name, goblin_engineer::address_t address);
        void create_database_finish(components::session::session_t& session, storage::database_create_result, goblin_engineer::address_t);
        void create_collection(components::session::session_t& session, std::string& database_name, std::string& collections_name, goblin_engineer::address_t address);
        void create_collection_finish(components::session::session_t& session, storage::collection_create_result, std::string& database_name, goblin_engineer::address_t);
        void drop_collection(components::session::session_t& session, std::string& database_name, std::string& collection_name, goblin_engineer::address_t address);
        void drop_collection_finish_collection(components::session::session_t& session, result_drop_collection& result, std::string& database_name, std::string& collection_name);
        void drop_collection_finish(components::session::session_t& session, result_drop_collection& result, std::string& database_name, goblin_engineer::address_t collection);
        void insert_one(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& document, goblin_engineer::address_t address);
        void insert_many(components::session::session_t& session, std::string& database_name, std::string& collection, std::list<components::document::document_t>& documents, goblin_engineer::address_t address);
        void insert_one_finish(components::session::session_t& session, result_insert_one& result);
        void insert_many_finish(components::session::session_t& session, result_insert_many& result);
        void find(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition, goblin_engineer::address_t address);
        void find_finish(components::session::session_t& session, components::cursor::sub_cursor_t* result);
        void find_one(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition, goblin_engineer::address_t address);
        void find_one_finish(components::session::session_t& session, result_find_one& result);
        void delete_one(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition, goblin_engineer::address_t address);
        void delete_many(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition, goblin_engineer::address_t address);
        void delete_finish(components::session::session_t& session, result_delete& result);
        void update_one(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition, components::document::document_t update, bool upsert, goblin_engineer::address_t address);
        void update_many(components::session::session_t& session, std::string& database_name, std::string& collection, components::document::document_t& condition, components::document::document_t update, bool upsert, goblin_engineer::address_t address);
        void update_finish(components::session::session_t& session, result_update& result);
        void size(components::session::session_t& session, std::string& database_name, std::string& collection, goblin_engineer::address_t address);
        void size_finish(components::session::session_t&, result_size& result);
        void close_cursor(components::session::session_t& session);

    private:
        log_t log_;
        goblin_engineer::address_t mdb_;
        std::unordered_map<components::session::session_t, goblin_engineer::address_t> session_to_address_;
        std::unordered_map<components::session::session_t, std::unique_ptr<components::cursor::cursor_t>> cursor_;
        std::unordered_map<key_collection_t, goblin_engineer::address_t, key_collection_t::hash> collection_address_book_;
        std::unordered_map<std::string, goblin_engineer::address_t> database_address_book_;
    };

} // namespace services::dispatcher