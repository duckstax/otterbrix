#pragma once

#include <actor-zeta.hpp>

#include <boost/filesystem.hpp>
#include <log/log.hpp>

#include <components/ql/statements.hpp>
#include <components/session/session.hpp>
#include <configuration/configuration.hpp>
#include <core/file/file_system.hpp>

#include "dto.hpp"
#include "forward.hpp"
#include "record.hpp"

namespace services::wal {

    class wal_replicate_t : public actor_zeta::basic_actor<wal_replicate_t> {
        using session_id_t = components::session::session_id_t;
        using address_t = actor_zeta::address_t;
        using file_ptr = std::unique_ptr<core::filesystem::file_handle_t>;

    public:
        wal_replicate_t(manager_wal_replicate_t* manager, log_t& log, configuration::config_wal config);
        void load(const session_id_t& session, address_t& sender, services::wal::id_t wal_id);
        void create_database(const session_id_t& session, address_t& sender, components::ql::create_database_t& data);
        void drop_database(const session_id_t& session, address_t& sender, components::ql::drop_database_t& data);
        void
        create_collection(const session_id_t& session, address_t& sender, components::ql::create_collection_t& data);
        void drop_collection(const session_id_t& session, address_t& sender, components::ql::drop_collection_t& data);
        void insert_one(const session_id_t& session, address_t& sender, components::ql::insert_one_t& data);
        void insert_many(const session_id_t& session, address_t& sender, components::ql::insert_many_t& data);
        void delete_one(const session_id_t& session, address_t& sender, components::ql::delete_one_t& data);
        void delete_many(const session_id_t& session, address_t& sender, components::ql::delete_many_t& data);
        void update_one(const session_id_t& session, address_t& sender, components::ql::update_one_t& data);
        void update_many(const session_id_t& session, address_t& sender, components::ql::update_many_t& data);
        void create_index(const session_id_t& session, address_t& sender, components::ql::create_index_t& data);
        ~wal_replicate_t() override;

        auto make_type() const noexcept -> const char* const;
        actor_zeta::behavior_t behavior();

    private:
        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t create_database_;
        actor_zeta::behavior_t drop_database_;
        actor_zeta::behavior_t create_collection_;
        actor_zeta::behavior_t drop_collection_;
        actor_zeta::behavior_t insert_one_;
        actor_zeta::behavior_t insert_many_;
        actor_zeta::behavior_t delete_one_;
        actor_zeta::behavior_t delete_many_;
        actor_zeta::behavior_t update_one_;
        actor_zeta::behavior_t update_many_;
        actor_zeta::behavior_t create_index_;

        void send_success(const session_id_t& session, address_t& sender);

        virtual void write_buffer(buffer_t& buffer);
        virtual void read_buffer(buffer_t& buffer, size_t start_index, size_t size) const;

        template<class T>
        void write_data_(T& data);

        void init_id();
        bool find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const;
        services::wal::id_t read_id(std::size_t start_index) const;
        record_t read_record(std::size_t start_index) const;
        size_tt read_size(size_t start_index) const;
        buffer_t read(size_t start_index, size_t finish_index) const;

        log_t log_;
        configuration::config_wal config_;
        atomic_id_t id_{0};
        crc32_t last_crc32_{0};
        core::filesystem::local_file_system_t fs_;
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

    class wal_replicate_without_disk_t final : public wal_replicate_t {
        using session_id_t = components::session::session_id_t;
        using address_t = actor_zeta::address_t;

    public:
        wal_replicate_without_disk_t(manager_wal_replicate_t* manager, log_t& log, configuration::config_wal config);
        void load(const session_id_t& session, address_t& sender, services::wal::id_t wal_id);

    private:
        void write_buffer(buffer_t&) final;
        void read_buffer(buffer_t& buffer, size_t start_index, size_t size) const final;
    };

    using wal_replicate_ptr = std::unique_ptr<wal_replicate_t, actor_zeta::pmr::deleter_t>;

} //namespace services::wal
