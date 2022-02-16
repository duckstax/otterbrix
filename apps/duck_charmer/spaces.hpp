#pragma once
#include <goblin-engineer/core.hpp>

#include "services/collection/collection.hpp"
#include "services/database/database.hpp"

#include "components/log/log.hpp"

#include "dispatcher/dispatcher.hpp"
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
        goblin_engineer::supervisor manager_database_;
        goblin_engineer::supervisor manager_dispatcher_;
        goblin_engineer::supervisor manager_wal_;
        std::unique_ptr<duck_charmer::wrapper_dispatcher_t> wrapper_dispatcher_;
    };
} // namespace duck_charmer