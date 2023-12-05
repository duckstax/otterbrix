#pragma once

#include <pybind11/pybind11.h>

#include <components/log/log.hpp>
#include <components/configuration/configuration.hpp>

#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/memory_storage/memory_storage.hpp>

#include "integration/cpp/wrapper_dispatcher.hpp"
#include "integration/cpp/base_spaces.hpp"

#include <core/excutor.hpp>

namespace otterbrix {

    class PYBIND11_EXPORT spaces final : public base_otterbrix_t {
    public:
        spaces(spaces& other) = delete;
        void operator=(const spaces&) = delete;

        static spaces* get_instance();

    protected:
        spaces();
    };

} // namespace python
