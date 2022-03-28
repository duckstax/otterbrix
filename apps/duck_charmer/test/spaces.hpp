#pragma once

#include <goblin-engineer/core.hpp>
#include <components/log/log.hpp>
#include <components/protocol/base.hpp>
#include <components/session/session.hpp>
#include <components/cursor/cursor.hpp>
#include <services/database/result_database.hpp>
#include <services/collection/result.hpp>

namespace test {

    class spin_lock final {
    public:
        spin_lock() = default;
        spin_lock(const spin_lock&) = delete;
        spin_lock(spin_lock&&) = default;
        spin_lock& operator=(const spin_lock&) = delete;
        spin_lock& operator=(spin_lock&&) = default;
        void lock() {
            while (_lock.test_and_set(std::memory_order_acquire)) continue;
        }
        void unlock() {
            _lock.clear(std::memory_order_release);
        }

    private:
        std::atomic_flag _lock = ATOMIC_FLAG_INIT;
    };

    using components::session::session_id_t;
    using manager_t = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;

    class spaces_t final : public manager_t {
    public:
        spaces_t(spaces_t& other) = delete;
        void operator=(const spaces_t&) = delete;

        static spaces_t &get();
        log_t& log();

        void create_database(const database_name_t &database);
        void create_collection(const database_name_t &database, const collection_name_t &collection);
        void drop_collection(const database_name_t &database, const collection_name_t &collection);
        void insert_one(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr &document);
        void insert_many(const database_name_t& database, const collection_name_t& collection, std::list<components::document::document_ptr> &documents);
        void find(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition);
        void find_one(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition);
        void delete_one(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition);
        void delete_many(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition);
        void update_one(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition, components::document::document_ptr update, bool upsert);
        void update_many(const database_name_t& database, const collection_name_t& collection, components::document::document_ptr condition, components::document::document_ptr update, bool upsert);
        size_t size(const database_name_t& database, const collection_name_t& collection);


    protected:
        auto add_actor_impl(goblin_engineer::actor) -> void final;
        auto add_supervisor_impl(goblin_engineer::supervisor) -> void final;
        auto executor_impl() noexcept -> goblin_engineer::abstract_executor* final;
        auto enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void final;

    private:
        spaces_t();

        log_t log_;
        goblin_engineer::supervisor manager_database_;
        goblin_engineer::supervisor manager_dispatcher_;
        goblin_engineer::supervisor manager_wal_;
        std::atomic_int i = 0;
        std::mutex output_mtx_;
        spin_lock input_mtx_;
        std::condition_variable cv_;
        session_id_t input_session_;
        session_id_t output_session_;
        std::variant<
            result_insert_one,
            result_insert_many,
            components::cursor::cursor_t*,
            result_find_one,
            result_size,
            result_delete,
            result_update,
            result_drop_collection,
            services::storage::database_create_result,
            services::storage::collection_create_result>
            intermediate_store_;

        auto create_database_finish(session_id_t&, services::storage::database_create_result) -> void;
        auto create_collection_finish(session_id_t& session, services::storage::collection_create_result) -> void;
        auto drop_collection_finish(session_id_t& session, result_drop_collection result) -> void;
        auto insert_one_finish(session_id_t& session, result_insert_one result) -> void;
        auto insert_many_finish(session_id_t& session, result_insert_many result) -> void;
        auto find_finish(session_id_t& session, components::cursor::cursor_t *cursor) -> void;
        auto find_one_finish(session_id_t& session, result_find_one result) -> void;
        auto delete_finish(session_id_t& session, result_delete result) -> void;
        auto update_finish(session_id_t& session, result_update result) -> void;
        auto size_finish(session_id_t& session, result_size result) -> void;

        void init();
        void wait();
        void notify();
    };

} //namespace test
