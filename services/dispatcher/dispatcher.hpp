#pragma once

#include <unordered_map>
#include <variant>

#include <actor-zeta.hpp>
#include <actor-zeta/detail/memory_resource.hpp>

#include <core/excutor.hpp>
#include <core/spinlock/spinlock.hpp>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/index.hpp>
#include <components/ql/statements/insert_many.hpp>
#include <components/ql/statements/insert_one.hpp>

#include <services/disk/result.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

#include "route.hpp"
#include "session.hpp"

namespace services::dispatcher {

    class manager_dispatcher_t;

    class dispatcher_t final : public actor_zeta::basic_async_actor {
    public:
        dispatcher_t(manager_dispatcher_t*,
                     std::pmr::memory_resource* resource,
                     actor_zeta::address_t,
                     actor_zeta::address_t,
                     actor_zeta::address_t,
                     log_t& log,
                     std::string name);
        ~dispatcher_t();
        void load(components::session::session_id_t& session, actor_zeta::address_t sender);
        void load_from_disk_result(components::session::session_id_t& session,
                                   const services::disk::result_load_t& result);
        void load_from_memory_resource_result(components::session::session_id_t& session,
                                              std::pmr::vector<collection_full_name_t> collections);
        void load_from_wal_result(components::session::session_id_t& session,
                                  std::vector<services::wal::record_t>& records);
        void execute_ql(components::session::session_id_t& session,
                        components::ql::ql_statement_t* ql,
                        actor_zeta::address_t address);
        void execute_ql_finish(components::session::session_id_t& session, components::cursor::cursor_t_ptr cursor);
        void create_collection_finish(components::session::session_id_t& session,
                                      collection_full_name_t collection_address);
        void size(components::session::session_id_t& session, std::string& database_name, std::string& collection);
        void size_finish(components::session::session_id_t&, components::cursor::cursor_t_ptr&& cursor);
        void close_cursor(components::session::session_id_t& session);
        void wal_success(components::session::session_id_t& session, services::wal::id_t wal_id);
        bool load_from_wal_in_progress(components::session::session_id_t& session);

    private:
        log_t log_;
        actor_zeta::address_t manager_dispatcher_;
        std::pmr::memory_resource* resource_;
        std::unordered_map<components::session::session_id_t, collection_full_name_t> last_collection_;
        actor_zeta::address_t memory_storage_;
        actor_zeta::address_t manager_wal_;
        actor_zeta::address_t manager_disk_;
        session_storage_t session_to_address_;
        std::unordered_map<components::session::session_id_t, std::unique_ptr<components::cursor::cursor_t>> cursor_;
        std::unordered_map<components::session::session_id_t, components::cursor::cursor_t_ptr>
            result_storage_; // to be able return result from wal_success
        disk::result_load_t load_result_;
        components::session::session_id_t load_session_;
        services::wal::id_t last_wal_id_{0};
        std::size_t load_count_answers_{0};

        std::pair<components::logical_plan::node_ptr, components::ql::storage_parameters>
        create_logic_plan(components::ql::ql_statement_t* statement);
        // TODO figure out what to do with records
        std::vector<services::wal::record_t> records_;
    };

    using dispatcher_ptr = std::unique_ptr<dispatcher_t>;

    class manager_dispatcher_t final : public actor_zeta::cooperative_supervisor<manager_dispatcher_t> {
    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t
        {
            memory_storage = 0,
            manager_wal = 1,
            manager_disk = 2
        };

        void sync(address_pack& pack) {
            memory_storage_ = std::get<static_cast<uint64_t>(unpack_rules::memory_storage)>(pack);
            manager_wal_ = std::get<static_cast<uint64_t>(unpack_rules::manager_wal)>(pack);
            manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
        }

        manager_dispatcher_t(actor_zeta::detail::pmr::memory_resource*, actor_zeta::scheduler_raw, log_t& log);

        ~manager_dispatcher_t() override;

        ///-----
        void create_dispatcher(const std::string& name_dispatcher) {
            actor_zeta::send(address(),
                             address(),
                             handler_id(route::create),
                             components::session::session_id_t(),
                             std::string(name_dispatcher));
        }
        ///------
        void create(components::session::session_id_t& session, std::string& name);
        void load(components::session::session_id_t& session);
        void execute_ql(components::session::session_id_t& session, components::ql::ql_statement_t* ql);
        void size(components::session::session_id_t& session, std::string& database_name, std::string& collection);
        void close_cursor(components::session::session_id_t& session);

    protected:
        auto scheduler_impl() noexcept -> actor_zeta::scheduler_abstract_t* final;
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override;

    private:
        spin_lock lock_;
        log_t log_;
        actor_zeta::scheduler_raw e_;
        actor_zeta::address_t memory_storage_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        std::vector<dispatcher_ptr> dispatchers_;

        auto dispatcher() -> actor_zeta::address_t;
    };

    using manager_dispatcher_ptr = std::unique_ptr<manager_dispatcher_t>;

} // namespace services::dispatcher
