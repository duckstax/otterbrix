#pragma once

#include <core/excutor.hpp>
#include <components/log/log.hpp>
#include <components/configuration/configuration.hpp>
#include "wrapper_dispatcher.hpp"

namespace services {

    namespace dispatcher {
        class manager_dispatcher_t;
        using manager_dispatcher_ptr = std::unique_ptr<manager_dispatcher_t>;
    }

    namespace disk {
        class base_manager_disk_t;
        using manager_disk_ptr = std::unique_ptr<base_manager_disk_t>;
    }

    namespace wal {
        class base_manager_wal_replicate_t;
        using manager_wal_ptr = std::unique_ptr<base_manager_wal_replicate_t>;
    }

    class memory_storage_t;
    using memory_storage_ptr = std::unique_ptr<memory_storage_t>;

} // namespace services

namespace duck_charmer {

    class base_spaces {
    public:
        base_spaces(base_spaces& other) = delete;
        void operator=(const base_spaces&) = delete;

        log_t& get_log();
        duck_charmer::wrapper_dispatcher_t* dispatcher();

        ~base_spaces();
        void init(const configuration::config_t& config);
    protected:
        explicit base_spaces();
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        actor_zeta::scheduler_ptr scheduler_dispather_;
        actor_zeta::detail::pmr::memory_resource* resource;
        services::dispatcher::manager_dispatcher_ptr manager_dispatcher_;
        services::disk::manager_disk_ptr manager_disk_;
        services::wal::manager_wal_ptr manager_wal_;
        services::memory_storage_ptr memory_storage_;
        std::unique_ptr<duck_charmer::wrapper_dispatcher_t> wrapper_dispatcher_;
    };

} // namespace python
