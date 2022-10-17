#pragma once

#include <components/ql/statements.hpp>
#include "dto.hpp"

namespace services::wal {

    struct record_t final {
        size_tt size;
        crc32_t crc32;
        crc32_t last_crc32;
        id_t id;
        components::ql::statement_type type;
        components::ql::variant_statement_t data;

        bool is_valid() const;
        void set_data(msgpack::object& object);
    };

} // namespace services::wal
