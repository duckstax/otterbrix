#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <variant>

#include <actor-zeta.hpp>

#include <core/spinlock/spinlock.hpp>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/ql/index.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/statements.hpp>

#include <services/collection/result.hpp>
#include <services/database/result_database.hpp>

namespace duck_charmer {

    using components::session::session_id_t;
    using components::document::document_ptr;

    class wrapper_dispatcher_t final : public actor_zeta::cooperative_supervisor<wrapper_dispatcher_t> {
    public:
        /// blocking method
        wrapper_dispatcher_t(actor_zeta::detail::pmr::memory_resource* , actor_zeta::address_t,log_t &log);
        ~wrapper_dispatcher_t();
        auto load() -> void;
        auto create_database(session_id_t &session, const database_name_t &database) -> void;
        auto create_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> void;
        auto drop_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_drop_collection;
        auto insert_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr &document) -> result_insert&;
        auto insert_many(session_id_t &session, const database_name_t &database, const collection_name_t &collection, std::pmr::vector<document_ptr> &documents) -> result_insert&;
        auto find(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> components::cursor::cursor_t*;
        auto find_one(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> result_find_one&;
        auto delete_one(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> result_delete&;
        auto delete_many(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> result_delete&;
        auto update_one(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition, document_ptr update, bool upsert) -> result_update&;
        auto update_many(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition, document_ptr update, bool upsert) -> result_update&;
        auto size(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> result_size;
        auto create_index(session_id_t &session, components::ql::create_index_t index) -> result_create_index;
        auto drop_index(session_id_t &session, components::ql::drop_index_t drop_index) -> result_drop_index;
        auto execute_ql(session_id_t& session, components::ql::variant_statement_t& ql) -> result_t;

    protected:

        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* final;
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        /// async method
        auto load_finish() -> void;
        auto create_database_finish(session_id_t &session, services::database::database_create_result result) -> void;
        auto create_collection_finish(session_id_t &session, services::database::collection_create_result result) -> void;
        auto drop_collection_finish(session_id_t &session, result_drop_collection result) -> void;
        auto insert_finish(session_id_t &session, result_insert result) -> void;
        auto find_finish(session_id_t &session, components::cursor::cursor_t *cursor) -> void;
        auto find_one_finish(session_id_t &session, result_find_one result) -> void;
        auto delete_finish(session_id_t &session, result_delete result) -> void;
        auto update_finish(session_id_t &session, result_update result) -> void;
        auto size_finish(session_id_t &session, result_size result) -> void;
        auto create_index_finish(session_id_t &session, result_create_index result) -> void;
        auto drop_index_finish(session_id_t &session, result_drop_index result) -> void;

        void init();
        void wait();
        void notify();

        actor_zeta::address_t manager_dispatcher_;
        log_t log_;
        std::atomic_int i = 0;
        std::mutex output_mtx_;
        spin_lock input_mtx_;
        std::condition_variable cv_;
        session_id_t input_session_;
        std::variant<
            null_result,
            result_insert,
            components::cursor::cursor_t*,
            result_find_one,
            result_size,
            result_delete,
            result_update,
            result_drop_collection,
            result_create_index,
            result_drop_index,
            services::database::database_create_result,
            services::database::collection_create_result>
            intermediate_store_;
    };
} // namespace python
