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
#include <components/ql/aggregate.hpp>
#include <components/ql/index.hpp>
#include <components/ql/statements.hpp>
#include <components/session/session.hpp>
#include <integration/cpp/impl/session_blocker.hpp>

namespace otterbrix {

    using components::document::document_ptr;
    using components::session::session_id_t;

    class wrapper_dispatcher_t final : public actor_zeta::cooperative_supervisor<wrapper_dispatcher_t> {
    public:
        /// blocking method
        wrapper_dispatcher_t(std::pmr::memory_resource*, actor_zeta::address_t, log_t& log);
        ~wrapper_dispatcher_t();
        auto load() -> void;
        [[deprecated]] auto create_database(const session_id_t& session, const database_name_t& database)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto drop_database(const session_id_t& session, const database_name_t& database)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto create_collection(const session_id_t& session,
                                              const database_name_t& database,
                                              const collection_name_t& collection) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto drop_collection(const session_id_t& session,
                                            const database_name_t& database,
                                            const collection_name_t& collection) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto insert_one(const session_id_t& session,
                                       const database_name_t& database,
                                       const collection_name_t& collection,
                                       document_ptr& document) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto insert_many(const session_id_t& session,
                                        const database_name_t& database,
                                        const collection_name_t& collection,
                                        std::pmr::vector<document_ptr>& documents) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto find(const session_id_t& session, components::ql::aggregate_statement_raw_ptr condition)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto find_one(const session_id_t& session, components::ql::aggregate_statement_raw_ptr condition)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto delete_one(const session_id_t& session,
                                       components::ql::aggregate_statement_raw_ptr condition)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto delete_many(const session_id_t& session,
                                        components::ql::aggregate_statement_raw_ptr condition)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto update_one(const session_id_t& session,
                                       components::ql::aggregate_statement_raw_ptr condition,
                                       document_ptr update,
                                       bool upsert) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto update_many(const session_id_t& session,
                                        components::ql::aggregate_statement_raw_ptr condition,
                                        document_ptr update,
                                        bool upsert) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto size(const session_id_t& session,
                                 const database_name_t& database,
                                 const collection_name_t& collection) -> size_t;
        auto create_index(const session_id_t& session, components::ql::create_index_t* ql)
            -> components::cursor::cursor_t_ptr;
        auto drop_index(const session_id_t& session, components::ql::drop_index_t* ql)
            -> components::cursor::cursor_t_ptr;
        auto execute_ql(const session_id_t& session, components::ql::variant_statement_t& query)
            -> components::cursor::cursor_t_ptr;
        auto execute_sql(const session_id_t& session, const std::string& query) -> components::cursor::cursor_t_ptr;

        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        // Behaviors
        actor_zeta::behavior_t load_finish_;
        actor_zeta::behavior_t execute_ql_finish_;
        actor_zeta::behavior_t size_finish_;
        /// async method
        auto load_finish(const session_id_t& session) -> void;
        auto execute_ql_finish(const session_id_t& session, components::cursor::cursor_t_ptr cursor) -> void;
        auto size_finish(const session_id_t& session, size_t size) -> void;

        void init(const session_id_t& session);
        void wait(const session_id_t& session);
        void notify(const session_id_t& session);

        template<typename Tql>
        auto send_ql(const session_id_t& session, Tql& ql, std::string_view title, uint64_t handle)
            -> components::cursor::cursor_t_ptr;

        auto send_ql_new(const session_id_t& session, components::ql::ql_statement_t* ql)
            -> components::cursor::cursor_t_ptr;

        actor_zeta::address_t manager_dispatcher_;
        log_t log_;
        std::atomic_int i = 0;
        std::mutex output_mtx_;
        spin_lock input_mtx_;
        std::condition_variable cv_;
        impl::session_block_t blocker_;
        components::cursor::cursor_t_ptr cursor_store_;
        size_t size_store_;
        bool bool_store_;
    };
} // namespace otterbrix