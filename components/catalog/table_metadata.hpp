#pragma once

#include "schema.hpp"

#include <chrono>

namespace components::catalog {
    struct table_metadata {
        table_metadata(std::pmr::memory_resource* resource, schema schema, const std::pmr::string& description = "");

        [[nodiscard]] const std::pmr::string& description() const;
        [[nodiscard]] field_id_t next_column_id() const;
        [[nodiscard]] timestamp last_updated_ms() const;
        [[nodiscard]] const schema& current_schema() const;

    private:
        schema schema_struct_;
        std::pmr::string table_description_;
        timestamp last_updated_ms_;
        field_id_t next_column_id_;
    };

    enum class used_format_t : uint8_t
    {
        documents = 0,
        columns = 1,
        undefined = 2
    };
} // namespace components::catalog
