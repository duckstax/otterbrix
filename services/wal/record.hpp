#pragma once

#include "dto.hpp"

#include <logical_plan/param_storage.hpp>

namespace services::wal {

    struct record_t final {
        size_tt size;
        crc32_t crc32;
        crc32_t last_crc32;
        id_t id;
        components::logical_plan::node_type type;
        components::logical_plan::node_ptr data;
        components::logical_plan::ql_param_statement_ptr params;

        bool is_valid() const;
        void set_data(msgpack::object& object, std::pmr::memory_resource* resource);
    };

} // namespace services::wal
