#pragma once

#include <pybind11/pybind11.h>

#include <components/log/log.hpp>
#include <components/configuration/configuration.hpp>

#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/database/database.hpp>

#include "integration/cpp/wrapper_dispatcher.hpp"
#include "integration/cpp/base_spaces.hpp"

#include <core/excutor.hpp>

namespace duck_charmer {

    class PYBIND11_EXPORT spaces final : public base_spaces {
    public:
        spaces(spaces& other) = delete;
        void operator=(const spaces&) = delete;

        static spaces* get_instance();

    protected:
        spaces();
        static spaces* instance_;
    };

} // namespace python