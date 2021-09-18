#pragma once
#include <goblin-engineer/core.hpp>

#include "services/storage/collection.hpp"
#include "services/storage/database.hpp"

#include "components/log/log.hpp"

#include "dispatcher.hpp"

class PYBIND11_EXPORT spaces final {
public:
    spaces(spaces& other) = delete;
    void operator=(const spaces&) = delete;

    static spaces* get_instance();
    log_t& get_log();
    goblin_engineer::actor_address dispatcher();

protected:
    spaces();

    static spaces* instance_;

    log_t log_;
    services::storage::manager_database_ptr manager_database_;
    services::storage::database_ptr database_;
    goblin_engineer::actor_address collection_;
    manager_dispatcher_ptr manager_dispatcher_;
    goblin_engineer::actor_address dispatcher_;
};