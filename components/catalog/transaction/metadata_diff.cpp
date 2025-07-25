#include "metadata_diff.hpp"

namespace components::catalog {
    metadata_diff::metadata_diff(std::pmr::memory_resource* resource)
        : schema_diff_(resource)
        , resource_(resource) {}

    void metadata_diff::update_description(const std::pmr::string& desc) { new_desc.emplace(desc); }

    void metadata_diff::use_schema_diff(schema_diff&& diff) { schema_diff_ = std::move(diff); }

    bool metadata_diff::has_changes() const { return new_desc || schema_diff_.has_changes(); }

    table_metadata metadata_diff::apply(const table_metadata& base_metadata) const {
        if (!has_changes()) {
            return base_metadata;
        }

        const schema& current_schema = base_metadata.current_schema();
        return table_metadata(resource_,
                              schema_diff_.has_changes() ? schema_diff_.apply(current_schema) : current_schema,
                              new_desc.value_or(base_metadata.description()));
    }
} // namespace components::catalog
