#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <goblin-engineer/core.hpp>
#include <mutex>
#include <variant>
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/protocol/base.hpp>
#include <services/collection/result.hpp>
#include <services/database/result_database.hpp>

class spin_lock final {
public:
    spin_lock() = default;
    spin_lock(const spin_lock&) = delete;
    spin_lock(spin_lock&&) = default;
    spin_lock &operator=(const spin_lock&) = delete;
    spin_lock &operator=(spin_lock&&) = default;
    void lock();
    void unlock();

private:
    std::atomic_flag _lock = ATOMIC_FLAG_INIT;
};

namespace duck_charmer {

    using manager_t = goblin_engineer::basic_manager_service_t<goblin_engineer::base_policy_light>;
    using components::session::session_id_t;
    using components::document::document_ptr;

    class wrapper_dispatcher_t final : public manager_t {
    public:
        /// blocking method
        explicit wrapper_dispatcher_t(log_t &log);
        auto create_database(session_id_t &session, const database_name_t &database) -> void;
        auto create_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> void;
        auto drop_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_drop_collection;
        auto insert_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr &document) -> result_insert_one&;
        auto insert_many(session_id_t &session, const database_name_t &database, const collection_name_t &collection, std::list<document_ptr> &documents) -> result_insert_many&;
        auto find(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition) -> components::cursor::cursor_t*;
        auto find_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition) -> result_find_one&;
        auto delete_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition) -> result_delete&;
        auto delete_many(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition) -> result_delete&;
        auto update_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition, document_ptr update, bool upsert) -> result_update&;
        auto update_many(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr condition, document_ptr update, bool upsert) -> result_update&;
        auto size(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_size;

    protected:
        auto add_actor_impl(goblin_engineer::actor) -> void final;
        auto add_supervisor_impl(goblin_engineer::supervisor) -> void final;
        auto executor_impl() noexcept -> goblin_engineer::abstract_executor* final;
        auto enqueue_base(actor_zeta::message_ptr msg, actor_zeta::execution_device*) -> void final;

    private:
        /// async method
        auto create_database_finish(session_id_t &session, services::storage::database_create_result result) -> void;
        auto create_collection_finish(session_id_t &session, services::storage::collection_create_result result) -> void;
        auto drop_collection_finish(session_id_t &session, result_drop_collection result) -> void;
        auto insert_one_finish(session_id_t &session, result_insert_one result) -> void;
        auto insert_many_finish(session_id_t &session, result_insert_many result) -> void;
        auto find_finish(session_id_t &session, components::cursor::cursor_t *cursor) -> void;
        auto find_one_finish(session_id_t &session, result_find_one result) -> void;
        auto delete_finish(session_id_t &session, result_delete result) -> void;
        auto update_finish(session_id_t &session, result_update result) -> void;
        auto size_finish(session_id_t &session, result_size result) -> void;

        void init();
        void wait();
        void notify();

        log_t log_;
        std::atomic_int i = 0;
        std::mutex output_mtx_;
        spin_lock input_mtx_;
        std::condition_variable cv_;
        session_id_t input_session_;
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
    };
} // namespace duck_charmer