#pragma once

#include "schema.hpp"
#include "table_id.hpp"

#include <chrono>

namespace components::catalog {
    struct table_metadata {
        table_metadata(std::pmr::memory_resource* resource, schema schema, const std::pmr::string& description = "");

        [[nodiscard]] const std::pmr::string& description() const;
        [[nodiscard]] field_id_t next_column_id() const;
        [[nodiscard]] timestamp last_updated_ms() const;
        [[nodiscard]] const schema& current_schema() const;

    private:
        schema schema_struct;
        std::pmr::string table_description;
        timestamp last_updated_ms_;
        field_id_t next_column_id_;
    };
} // namespace components::catalog
