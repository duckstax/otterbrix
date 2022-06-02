#pragma once

#include <actor-zeta.hpp>

#include <boost/filesystem.hpp>
#include <log/log.hpp>

#include <components/session/session.hpp>
#include <components/protocol/protocol.hpp>
#include <components/document/file.hpp>

#include "dto.hpp"
#include "record.hpp"
#include "manager_wal_replicate.hpp"

namespace services::wal {

    class wal_replicate_t final : public actor_zeta::basic_async_actor {
        using session_id_t = components::session::session_id_t;
        using address_t = actor_zeta::address_t;
        using file_ptr = std::unique_ptr<components::file::file_t>;

    public:
        wal_replicate_t(manager_wal_replicate_t* manager, log_t& log, boost::filesystem::path path);
        void load(session_id_t& session, address_t& sender, services::wal::id_t wal_id);
        void create_database(session_id_t& session, address_t& sender, components::protocol::create_database_t& data);
        void drop_database(session_id_t& session, address_t& sender, components::protocol::drop_database_t& data);
        void create_collection(session_id_t& session, address_t& sender, components::protocol::create_collection_t& data);
        void drop_collection(session_id_t& session, address_t& sender, components::protocol::drop_collection_t& data);
        void insert_one(session_id_t& session, address_t& sender, insert_one_t& data);
        void insert_many(session_id_t& session, address_t& sender, insert_many_t& data);
//        void delete_one(database_name_t& database, collection_name_t& collection, components::document::document_t& condition) {}
//        void delete_many(database_name_t& database, collection_name_t& collection, components::document::document_t& condition) {}
//        void update_one(database_name_t& database, collection_name_t& collection, components::document::document_t& condition, components::document::document_t update, bool upsert) {}
//        void update_many(database_name_t& database, collection_name_t& collection, components::document::document_t& condition, components::document::document_t update, bool upsert) {}
        ~wal_replicate_t() override;

    private:
        void send_success(session_id_t& session, address_t& sender);

        template <class T>
        void write_data_(T &data);

        void init_id();
        bool find_start_record(services::wal::id_t wal_id, std::size_t &start_index) const;
        services::wal::id_t read_id(std::size_t start_index) const;
        record_t read_record(std::size_t start_index) const;

        log_t log_;
        boost::filesystem::path path_;
        atomic_id_t id_{0};
        crc32_t last_crc32_{0};
        file_ptr file_;
        buffer_t buffer_;
#ifdef DEV_MODE
    public:
        size_tt read_size(size_t start_index) const;
        buffer_t read(size_t start_index, size_t finish_index) const;
#endif
    };

} //namespace services::wal
