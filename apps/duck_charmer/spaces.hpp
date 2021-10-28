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
    goblin_engineer::address_t dispatcher();

protected:
    spaces();
    static spaces* instance_;

    log_t log_;
    goblin_engineer::supervisor manager_database_;
    goblin_engineer::supervisor manager_dispatcher_;
};