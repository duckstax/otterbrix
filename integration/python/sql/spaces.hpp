#pragma once

#include <pybind11/pybind11.h>

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>

#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/memory_storage/memory_storage.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include "integration/cpp/base_spaces.hpp"
#include "integration/cpp/wrapper_dispatcher.hpp"

#include <core/excutor.hpp>

namespace otterbrix {

    class PYBIND11_EXPORT spaces final
        : public base_otterbrix_t
        , public boost::intrusive_ref_counter<spaces> {
    public:
        spaces(spaces& other) = delete;
        void operator=(const spaces&) = delete;

        static boost::intrusive_ptr<spaces> get_instance();
        static boost::intrusive_ptr<spaces> get_instance(const std::filesystem::path& path);

    protected:
        spaces();
        spaces(const std::filesystem::path& path);
    };

    using spaces_ptr = boost::intrusive_ptr<spaces>;

} // namespace otterbrix
