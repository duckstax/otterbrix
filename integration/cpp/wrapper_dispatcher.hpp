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
#include <components/result/result.hpp>

namespace otterbrix {

    using components::session::session_id_t;
    using components::document::document_ptr;

    class wrapper_dispatcher_t final : public actor_zeta::cooperative_supervisor<wrapper_dispatcher_t> {
    public:
        /// blocking method
        wrapper_dispatcher_t(actor_zeta::detail::pmr::memory_resource* , actor_zeta::address_t,log_t &log);
        ~wrapper_dispatcher_t();
        auto load() -> void;
        [[deprecated]] auto create_database(session_id_t &session, const database_name_t &database) -> components::result::result_t;
        [[deprecated]] auto drop_database(session_id_t &session, const database_name_t &database) -> components::result::result_t;
        [[deprecated]] auto create_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> components::result::result_t;
        [[deprecated]] auto drop_collection(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> components::result::result_t;
        [[deprecated]] auto insert_one(session_id_t &session, const database_name_t &database, const collection_name_t &collection, document_ptr &document) -> components::result::result_t&;
        [[deprecated]] auto insert_many(session_id_t &session, const database_name_t &database, const collection_name_t &collection, std::pmr::vector<document_ptr> &documents) -> components::result::result_t&;
        [[deprecated]] auto find(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> components::result::result_t;
        [[deprecated]] auto find_one(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> components::result::result_t;
        [[deprecated]] auto delete_one(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> components::result::result_delete&;
        [[deprecated]] auto delete_many(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition) -> components::result::result_delete&;
        [[deprecated]] auto update_one(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition, document_ptr update, bool upsert) -> components::result::result_update&;
        [[deprecated]] auto update_many(session_id_t &session, components::ql::aggregate_statement_raw_ptr condition, document_ptr update, bool upsert) -> components::result::result_update&;
        [[deprecated]] auto size(session_id_t &session, const database_name_t &database, const collection_name_t &collection) -> components::result::result_size;
        auto create_index(session_id_t &session, components::ql::create_index_t index) -> components::result::result_create_index;
        auto drop_index(session_id_t &session, components::ql::drop_index_t drop_index) -> components::result::result_drop_index;
        auto execute_ql(session_id_t& session, components::ql::variant_statement_t& query) -> components::result::result_t;
        auto execute_sql(session_id_t& session, const std::string& query) -> components::result::result_t;

    protected:

        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* final;
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        /// async method
        auto load_finish() -> void;
        auto execute_ql_finish(session_id_t &session, const components::result::result_t& result) -> void;
        auto insert_finish(session_id_t &session, components::result::result_insert result) -> void;
        auto delete_finish(session_id_t &session, components::result::result_delete result) -> void;
        auto update_finish(session_id_t &session, components::result::result_update result) -> void;
        auto size_finish(session_id_t &session, components::result::result_size result) -> void;
        auto create_index_finish(session_id_t &session, components::result::result_create_index result) -> void;
        auto drop_index_finish(session_id_t &session, components::result::result_drop_index result) -> void;

        void init();
        void wait();
        void notify();

        template <typename Tres, typename Tql>
        auto send_ql(session_id_t &session, Tql& ql, std::string_view title, uint64_t handle) -> components::result::result_t;

        auto send_ql_new(session_id_t &session, components::ql::ql_statement_t* ql) -> components::result::result_t;

        actor_zeta::address_t manager_dispatcher_;
        log_t log_;
        std::atomic_int i = 0;
        std::mutex output_mtx_;
        spin_lock input_mtx_;
        std::condition_variable cv_;
        session_id_t input_session_;
        std::variant<
            components::result::empty_result_t,
            components::result::result_insert,
            components::result::result_size,
            components::result::result_delete,
            components::result::result_update,
            components::result::result_drop_collection,
            components::result::result_create_index,
            components::result::result_drop_index,
            components::result::result_t>
            intermediate_store_;
    };
} // namespace python
