#pragma once

#include "dto.hpp"

#include <components/logical_plan/param_storage.hpp>

namespace services::wal {

    struct record_t final {
        size_tt size;
        crc32_t crc32;
        crc32_t last_crc32;
        id_t id;
        components::logical_plan::node_ptr data;
        components::logical_plan::parameter_node_ptr params;

        bool is_valid() const { return size > 0; }
    };

} // namespace services::wal
