#pragma once

#include <core/excutor.hpp>
#include <components/log/log.hpp>
#include <components/configuration/configuration.hpp>
#include "wrapper_dispatcher.hpp"

namespace services {

    namespace dispatcher {
        class manager_dispatcher_t;
        using manager_dispatcher_ptr = std::unique_ptr<manager_dispatcher_t,actor_zeta::deleter>;
    }

    namespace disk {
        class manager_disk_t;
        using manager_disk_ptr = std::unique_ptr<manager_disk_t,actor_zeta::deleter>;
        class manager_disk_empty_t;
        using manager_disk_empty_ptr = std::unique_ptr<manager_disk_empty_t,actor_zeta::deleter>;
    }

    namespace wal {
        class manager_wal_replicate_t;
        class manager_wal_replicate_empty_t;
        using manager_wal_ptr = std::unique_ptr<manager_wal_replicate_t,actor_zeta::deleter>;
        using manager_wal_empty_ptr = std::unique_ptr<manager_wal_replicate_empty_t,actor_zeta::deleter>;
    }

    class memory_storage_t;
    using memory_storage_ptr = std::unique_ptr<memory_storage_t,actor_zeta::deleter>;

} // namespace services

namespace duck_charmer {


    class base_spaces {
    public:
        base_spaces(base_spaces& other) = delete;
        void operator=(const base_spaces&) = delete;

        log_t& get_log();
        duck_charmer::wrapper_dispatcher_t* dispatcher();

        ~base_spaces();

    protected:
        explicit base_spaces(const configuration::config& config);

        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        actor_zeta::scheduler_ptr scheduler_dispather_;
        std::pmr::memory_resource* resource;
        services::dispatcher::manager_dispatcher_ptr manager_dispatcher_;
        services::memory_storage_ptr memory_storage_;
        std::variant<std::monostate,services::disk::manager_disk_empty_ptr,services::disk::manager_disk_ptr> manager_disk_;
        std::variant<std::monostate,services::wal::manager_wal_empty_ptr,services::wal::manager_wal_ptr> manager_wal_;
        std::unique_ptr<duck_charmer::wrapper_dispatcher_t,actor_zeta::deleter> wrapper_dispatcher_;
    };

} // namespace python
