#include "table_metadata.hpp"

namespace components::catalog {
    table_metadata::table_metadata(std::pmr::memory_resource* resource,
                                   schema schema,
                                   const std::pmr::string& description)
        : schema_struct_(std::move(schema))
        , table_description_(description, resource)
        , last_updated_ms_(std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()))
        , next_column_id_(schema_struct_.highest_field_id() + 1) {}

    const std::pmr::string& table_metadata::description() const { return table_description_; }

    field_id_t table_metadata::next_column_id() const { return next_column_id_; }

    timestamp table_metadata::last_updated_ms() const { return last_updated_ms_; }

    const schema& table_metadata::current_schema() const { return schema_struct_; }
} // namespace components::catalog
