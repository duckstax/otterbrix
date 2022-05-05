#pragma once

#include <pybind11.h>

#include <components/log/log.hpp>

#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/database/database.hpp>

#include "wrapper_dispatcher.hpp"

namespace duck_charmer {
    class PYBIND11_EXPORT spaces final {
    public:
        spaces(spaces& other) = delete;
        void operator=(const spaces&) = delete;

        static spaces* get_instance();
        log_t& get_log();
        duck_charmer::wrapper_dispatcher_t* dispatcher();

    protected:
        spaces();
        static spaces* instance_;

        log_t log_;
        actor_zeta::detail::pmr::memory_resource* resource;
        services::database::manager_database_ptr manager_database_;
        services::dispatcher::manager_dispatcher_ptr manager_dispatcher_;
        services::wal::manager_wal_ptr manager_wal_;
        services::disk::manager_disk_ptr manager_disk_;
        std::unique_ptr<duck_charmer::wrapper_dispatcher_t> wrapper_dispatcher_;
    };
} // namespace duck_charmer