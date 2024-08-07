#pragma once

#include "dto.hpp"
#include <components/ql/statements.hpp>

namespace services::wal {

    struct record_t final {
        size_tt size;
        crc32_t crc32;
        crc32_t last_crc32;
        id_t id;
        components::ql::statement_type type;
        components::ql::variant_statement_t data;

        bool is_valid() const;
        void set_data(msgpack::object& object, std::pmr::memory_resource* resource);
    };

} // namespace services::wal
