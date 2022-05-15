#pragma once

#include <unordered_map>
#include <variant>

#include <actor-zeta.hpp>
#include <actor-zeta/detail/memory_resource.hpp>

#include <core/spinlock/spinlock.hpp>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/log/log.hpp>
#include <core/excutor.hpp>

#include <services/collection/result.hpp>
#include <services/database/forward.hpp>
#include <services/database/result_database.hpp>
#include <services/wal/base.hpp>

#include "route.hpp"
#include "session.hpp"

namespace services::dispatcher {

    class manager_dispatcher_t final : public actor_zeta::cooperative_supervisor<manager_dispatcher_t> {
    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t {
            manager_database = 0,
            manager_wal = 1,
            manager_disk = 2
        };

        void sync(address_pack& pack) {
            manager_database_ = std::get<static_cast<uint64_t>(unpack_rules::manager_database)>(pack);
            manager_wal_ = std::get<static_cast<uint64_t>(unpack_rules::manager_wal)>(pack);
            manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
        }

        manager_dispatcher_t(
            actor_zeta::detail::pmr::memory_resource*,
            actor_zeta::scheduler_raw,
            log_t& log);

        ~manager_dispatcher_t() override;

        ///-----
        void create_dispatcher(const std::string& name_dispatcher) {
            actor_zeta::send(
                address(),
                address(),
                handler_id(route::create),
                components::session::session_id_t(),
                std::string(name_dispatcher));
        }
        ///------
        void create(components::session::session_id_t& session, std::string& name);
        void create_database(components::session::session_id_t& session, std::string& name);
        void create_collection(components::session::session_id_t& session, std::string& database_name, std::string& collection_name);
        void drop_collection(components::session::session_id_t& session, std::string& database_name, std::string& collection_name);
        void insert_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& document);
        void insert_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, std::list<components::document::document_ptr>& documents);
        void find(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition);
        void find_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition);
        void delete_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition);
        void delete_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition);
        void update_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, components::document::document_ptr& update, bool upsert);
        void update_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, components::document::document_ptr& update, bool upsert);
        void size(components::session::session_id_t& session, std::string& database_name, std::string& collection);
        void close_cursor(components::session::session_id_t& session);

    protected:
        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* final;
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override;

    private:
        spin_lock lock_;
        log_t log_;
        actor_zeta::scheduler_raw e_;
        actor_zeta::address_t manager_database_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        std::vector<actor_zeta::actor> actor_storage_;
        std::unordered_map<std::string, actor_zeta::address_t> dispatcher_to_address_book_;
        std::vector<actor_zeta::address_t> dispathers_;
    };

    using manager_dispatcher_ptr = std::unique_ptr<manager_dispatcher_t>;

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

    class dispatcher_t final : public actor_zeta::basic_async_actor {
    public:
        dispatcher_t(manager_dispatcher_t*, actor_zeta::address_t, actor_zeta::address_t, actor_zeta::address_t, log_t& log, std::string name);
        void create_database(components::session::session_id_t& session, std::string& name, actor_zeta::address_t address);
        void create_database_finish(components::session::session_id_t& session, database::database_create_result, std::string& database_name, actor_zeta::address_t);
        void create_collection(components::session::session_id_t& session, std::string& database_name, std::string& collections_name, actor_zeta::address_t address);
        void create_collection_finish(components::session::session_id_t& session, database::collection_create_result, std::string& database_name,std::string& collection_name, actor_zeta::address_t);
        void drop_collection(components::session::session_id_t& session, std::string& database_name, std::string& collection_name, actor_zeta::address_t address);
        void drop_collection_finish_collection(components::session::session_id_t& session, result_drop_collection& result, std::string& database_name, std::string& collection_name);
        void drop_collection_finish(components::session::session_id_t& session, result_drop_collection& result, std::string& database_name,std::string& collection_name, actor_zeta::address_t collection);
        void insert_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& document, actor_zeta::address_t address);
        void insert_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, std::list<components::document::document_ptr>& documents, actor_zeta::address_t address);
        void insert_one_finish(components::session::session_id_t& session, result_insert_one& result);
        void insert_many_finish(components::session::session_id_t& session, result_insert_many& result);
        void find(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, actor_zeta::address_t address);
        void find_finish(components::session::session_id_t& session, components::cursor::sub_cursor_t* result);
        void find_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, actor_zeta::address_t address);
        void find_one_finish(components::session::session_id_t& session, result_find_one& result);
        void delete_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, actor_zeta::address_t address);
        void delete_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, actor_zeta::address_t address);
        void delete_finish(components::session::session_id_t& session, result_delete& result);
        void update_one(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, components::document::document_ptr& update, bool upsert, actor_zeta::address_t address);
        void update_many(components::session::session_id_t& session, std::string& database_name, std::string& collection, components::document::document_ptr& condition, components::document::document_ptr& update, bool upsert, actor_zeta::address_t address);
        void update_finish(components::session::session_id_t& session, result_update& result);
        void size(components::session::session_id_t& session, std::string& database_name, std::string& collection, actor_zeta::address_t address);
        void size_finish(components::session::session_id_t&, result_size& result);
        void close_cursor(components::session::session_id_t& session);
        void wal_success(components::session::session_id_t& session, services::wal::id_t wal_id);

    private:
        log_t log_;
        actor_zeta::address_t manager_dispatcher_;
        actor_zeta::address_t manager_database_;
        actor_zeta::address_t manager_wal_;
        actor_zeta::address_t manager_disk_;
        session_storage_t session_to_address_;
        std::unordered_map<components::session::session_id_t, std::unique_ptr<components::cursor::cursor_t>> cursor_;
        std::unordered_map<key_collection_t, actor_zeta::address_t, key_collection_t::hash> collection_address_book_;
        std::unordered_map<std::string, actor_zeta::address_t> database_address_book_;
    };

} // namespace services::dispatcher