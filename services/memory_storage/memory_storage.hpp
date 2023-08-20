#pragma once

#include <memory_resource>
#include <core/btree/btree.hpp>
#include <core/excutor.hpp>
#include <core/spinlock/spinlock.hpp>
#include <components/log/log.hpp>
#include <components/ql/base.hpp>
#include <components/ql/ql_statement.hpp>
#include <components/session/session.hpp>

namespace components::ql {
    struct create_database_t;
    struct drop_database_t;
    struct create_collection_t;
    struct drop_collection_t;
}

namespace services {

    class memory_storage_t final : public actor_zeta::cooperative_supervisor<memory_storage_t> {
        using database_storage_t = std::pmr::set<database_name_t>;
        using collection_storage_t = core::pmr::btree::btree_t<collection_full_name_t, actor_zeta::address_t>;

    public:
        using address_pack = std::tuple<actor_zeta::address_t>;
        enum class unpack_rules : uint64_t {
            manager_dispatcher = 0,
        };

        memory_storage_t(actor_zeta::detail::pmr::memory_resource* resource, actor_zeta::scheduler_raw scheduler, log_t& log);
        ~memory_storage_t();

        void sync(const address_pack& pack);
        void execute_ql(components::session::session_id_t& session, components::ql::ql_statement_t* ql);

        actor_zeta::scheduler_abstract_t* scheduler_impl() noexcept final;
        void enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit* unit) final;

    private:
        spin_lock lock_;
        actor_zeta::address_t manager_dispatcher_{actor_zeta::address_t::empty_address()};
        log_t log_;
        actor_zeta::scheduler_raw e_;
        database_storage_t databases_;
        collection_storage_t collections_;

        void create_database_(components::session::session_id_t& session, components::ql::create_database_t* ql);
        void drop_database_(components::session::session_id_t& session, components::ql::drop_database_t* ql);
        void create_collection_(components::session::session_id_t& session, components::ql::create_collection_t* ql);
        void drop_collection_(components::session::session_id_t& session, components::ql::drop_collection_t* ql);
    };

} // namespace services
