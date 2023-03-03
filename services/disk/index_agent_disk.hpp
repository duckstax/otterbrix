#pragma once

#include <filesystem>
#include <core/excutor.hpp>
#include <core/btree/btree.hpp>
#include <configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include "command.hpp"

namespace rocksdb {
    class DB;
}

namespace services::disk {

    using index_name_t = std::string;
    using session_id_t = ::components::session::session_id_t;
    using path_t = std::filesystem::path;

    class base_manager_disk_t;

    class index_agent_disk_t final : public actor_zeta::basic_async_actor {
    public:
        index_agent_disk_t(base_manager_disk_t*, const path_t&, const collection_name_t&, const index_name_t&, log_t&);
        ~index_agent_disk_t() final;

    private:
        log_t log_;
        std::unique_ptr<rocksdb::DB> db_;
    };

    using index_agent_disk_ptr = std::unique_ptr<index_agent_disk_t>;
    using index_agent_disk_storage_t = core::pmr::btree::btree_t<index_name_t, index_agent_disk_ptr>;

} //namespace services::disk