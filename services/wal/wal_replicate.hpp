#pragma once

#include <actor-zeta.hpp>

#include <log/log.hpp>

#include <components/ql/statements.hpp>
#include <components/session/session.hpp>
#include <configuration/configuration.hpp>
#include <core/excutor.hpp>
#include <core/file/file.hpp>
#include <core/spinlock/spinlock.hpp>
#include <core/system_command.hpp>

#include "base.hpp"
#include "dto.hpp"
#include "record.hpp"
#include "route.hpp"

namespace services::wal {

    constexpr static auto wal_name = ".wal";

    bool file_exist_(const std::filesystem::path& path);

    class base_wal_replicate_t : public actor_zeta::basic_async_actor {
    protected:
        template<typename T>
        base_wal_replicate_t(actor_zeta::cooperative_supervisor<T>* supervisor, const std::string& name)
            : actor_zeta::basic_async_actor(supervisor, name) {}
    };

    using wal_ptr = std::unique_ptr<base_wal_replicate_t>;

    class wal_replicate_t final : public base_wal_replicate_t {
        using session_id_t = components::session::session_id_t;
        using address_t = actor_zeta::address_t;
        using file_ptr = std::unique_ptr<core::file::file_t>;

    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t
        {
            manager_disk = 0,
            memory_storage = 1
        };

        void sync(address_pack& pack) {
            manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
            memory_storage_ = std::get<static_cast<uint64_t>(unpack_rules::memory_storage)>(pack);
        }

        template<typename T>
        wal_replicate_t(actor_zeta::cooperative_supervisor<T>* supervisor, configuration::config_wal, log_t&);
        ~wal_replicate_t() final;

        void load(session_id_t& session, services::wal::id_t wal_id);
        void create_database(session_id_t& session, components::ql::create_database_t& data);
        void drop_database(session_id_t& session, components::ql::drop_database_t& data);
        void create_collection(session_id_t& session, components::ql::create_collection_t& data);
        void drop_collection(session_id_t& session, components::ql::drop_collection_t& data);
        void insert_one(session_id_t& session, components::ql::insert_one_t& data);
        void insert_many(session_id_t& session, components::ql::insert_many_t& data);
        void delete_one(session_id_t& session, components::ql::delete_one_t& data);
        void delete_many(session_id_t& session, components::ql::delete_many_t& data);
        void update_one(session_id_t& session, components::ql::update_one_t& data);
        void update_many(session_id_t& session, components::ql::update_many_t& data);
        void create_index(session_id_t& session, components::ql::create_index_t& data);

    private:
        void send_success_(session_id_t& session);

        void write_buffer(buffer_t& buffer);
        void read_buffer(buffer_t& buffer, size_t start_index, size_t size) const;

        // for wal tests without proper suervisor
#ifdef DEV_MODE
    public:
        template<class T>
        void write_data_(T& data);

    private:
#else
        template<class T>
        void write_data_(T& data);
#endif

        void init_id();
        bool find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const;
        services::wal::id_t read_id(std::size_t start_index) const;
        record_t read_record(std::size_t start_index) const;
        size_tt read_size(size_t start_index) const;
        buffer_t read(size_t start_index, size_t finish_index) const;

        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t memory_storage_ = actor_zeta::address_t::empty_address();
        configuration::config_wal config_;
        log_t log_;

        atomic_id_t id_{0};
        crc32_t last_crc32_{0};
        file_ptr file_;

#ifdef DEV_MODE
    public:
        bool test_find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const;
        services::wal::id_t test_read_id(std::size_t start_index) const;
        std::size_t test_next_record(std::size_t start_index) const;
        record_t test_read_record(std::size_t start_index) const;
        size_tt test_read_size(size_t start_index) const;
        buffer_t test_read(size_t start_index, size_t finish_index) const;
#endif
    };

    class wal_replicate_empty_t final : public base_wal_replicate_t {
        using session_id_t = components::session::session_id_t;
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

    public:
        template<typename T>
        wal_replicate_empty_t(actor_zeta::cooperative_supervisor<T>* supervisor, log_t&);

        template<class T>
        auto always_success(session_id_t& session, T&&) -> void {
            trace(log_, "wal_replicate_empty_t::always_success: session: {}", session.data()); // Works fine
            actor_zeta::send(current_message()->sender(),
                             address(),
                             services::wal::handler_id(services::wal::route::success),
                             session,
                             services::wal::id_t(0));
            trace(log_, "wal_replicate_empty_t::always_success: sent finished"); // is not called
        }

        template<class... Args>
        auto nothing(Args&&...) -> void {}

    private:
        log_t log_;
    };

    template<typename T>
    wal_replicate_t::wal_replicate_t(actor_zeta::cooperative_supervisor<T>* supervisor,
                                     configuration::config_wal config,
                                     log_t& log)
        : base_wal_replicate_t(supervisor, "wal_replicate_t")
        , config_(std::move(config))
        , log_(log.clone()) {
        trace(log_, "wal_replicate_t");
        add_handler(handler_id(route::load), &wal_replicate_t::load);
        add_handler(handler_id(route::create_database), &wal_replicate_t::create_database);
        add_handler(handler_id(route::drop_database), &wal_replicate_t::drop_database);
        add_handler(handler_id(route::create_collection), &wal_replicate_t::create_collection);
        add_handler(handler_id(route::drop_collection), &wal_replicate_t::drop_collection);
        add_handler(handler_id(route::insert_one), &wal_replicate_t::insert_one);
        add_handler(handler_id(route::insert_many), &wal_replicate_t::insert_many);
        add_handler(handler_id(route::delete_one), &wal_replicate_t::delete_one);
        add_handler(handler_id(route::delete_many), &wal_replicate_t::delete_many);
        add_handler(handler_id(route::update_one), &wal_replicate_t::update_one);
        add_handler(handler_id(route::update_many), &wal_replicate_t::update_many);
        add_handler(core::handler_id(core::route::sync), &wal_replicate_t::sync);
        add_handler(handler_id(route::create_index), &wal_replicate_t::create_index);
        trace(log_, "wal_replicate_t start thread pool");
        if (config_.sync_to_disk) {
            if (!file_exist_(config_.path)) {
                std::filesystem::create_directory(config_.path);
            }
            file_ = std::make_unique<core::file::file_t>(config_.path / wal_name);
            file_->seek_eof();
            init_id();
        }
    }

    template<typename T>
    wal_replicate_empty_t::wal_replicate_empty_t(actor_zeta::cooperative_supervisor<T>* supervisor, log_t& log)
        : base_wal_replicate_t(supervisor, "wal_replicate_empty_t")
        , log_(log.clone()) {
        trace(log_, "wal_replicate_empty_t");
        using namespace components;
        add_handler(handler_id(route::load), &wal_replicate_empty_t::always_success<wal::id_t>);
        add_handler(handler_id(route::create_database), &wal_replicate_empty_t::always_success<ql::create_database_t&>);
        add_handler(handler_id(route::drop_database), &wal_replicate_empty_t::always_success<ql::drop_database_t&>);
        add_handler(handler_id(route::create_collection),
                    &wal_replicate_empty_t::always_success<ql::create_collection_t&>);
        add_handler(handler_id(route::drop_collection), &wal_replicate_empty_t::always_success<ql::drop_collection_t&>);
        add_handler(handler_id(route::insert_one), &wal_replicate_empty_t::always_success<ql::insert_one_t&>);
        add_handler(handler_id(route::insert_many), &wal_replicate_empty_t::always_success<ql::insert_many_t&>);
        add_handler(handler_id(route::delete_one), &wal_replicate_empty_t::always_success<ql::delete_one_t&>);
        add_handler(handler_id(route::delete_many), &wal_replicate_empty_t::always_success<ql::delete_many_t&>);
        add_handler(handler_id(route::update_one), &wal_replicate_empty_t::always_success<ql::update_one_t&>);
        add_handler(handler_id(route::update_many), &wal_replicate_empty_t::always_success<ql::update_many_t&>);
        add_handler(handler_id(route::create_index), &wal_replicate_empty_t::always_success<ql::create_index_t&>);

        add_handler(handler_id(route::create), &wal_replicate_empty_t::nothing<>);
        add_handler(core::handler_id(core::route::sync), &wal_replicate_empty_t::nothing<address_pack&>);
    }

#ifdef DEV_MODE
    class test_wal_supervisor_t : public actor_zeta::cooperative_supervisor<test_wal_supervisor_t> {
    public:
        test_wal_supervisor_t(actor_zeta::detail::pmr::memory_resource* resource,
                              actor_zeta::scheduler_raw scheduler,
                              const configuration::config_wal& config,
                              log_t& log);

        template<class T>
        void write_data(T& data) {
            wal->write_data_(data);
        }

        std::unique_ptr<wal_replicate_t> wal{nullptr};

    private:
        actor_zeta::scheduler_abstract_t* scheduler_impl() noexcept final;
        void enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit* unit) final;

        actor_zeta::scheduler_raw e_;
        spin_lock lock_;
        actor_zeta::address_t wal_address_{actor_zeta::address_t::empty_address()};
    };
#endif

} //namespace services::wal
