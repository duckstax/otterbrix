#include "schema.hpp"

using namespace components::types;

namespace components::catalog {
    schema::schema(std::pmr::memory_resource* resource,
                   const components::types::complex_logical_type& schema_struct,
                   const std::pmr::vector<field_id_t>& primary_key)
        : schema_struct(schema_struct)
        , primary_key_field_ids(primary_key, resource)
        , id_to_struct_idx(resource) {
        auto& detailed_struct = to_struct(schema_struct);
        {
            std::pmr::unordered_set<std::pmr::string> names(resource);
            for (const auto& type : detailed_struct.child_types()) {
                if (names.find(type.alias().c_str()) != names.end()) {
                    throw schema_exception("Duplicate column with name \"" + type.alias() + "\", names must be unique");
                }

                names.emplace(type.alias());
            }
        }

        {
            field_id_t max = 0;
            size_t idx = 0;
            for (const auto& desc : detailed_struct.descriptions()) {
                if (id_to_struct_idx.find(desc.field_id) != id_to_struct_idx.end()) {
                    throw schema_exception("Duplicate id in schema: " + std::to_string(desc.field_id) +
                                           ", ids must be unique");
                }

                id_to_struct_idx.emplace(desc.field_id, idx++);
                max = std::max(max, desc.field_id);
            }

            for (field_id_t key : primary_key_field_ids) {
                if (id_to_struct_idx.find(key) == id_to_struct_idx.end()) {
                    throw schema_exception("No field with such id: " + std::to_string(key));
                }
            }

            highest = max;
        }
    }

    const complex_logical_type& schema::find_field(const std::pmr::string& name) const {
        return schema_struct.child_types()[find_idx_by_name(name)];
    }

    const types::field_description& schema::get_field_description(const std::pmr::string& name) const {
        return to_struct(schema_struct).descriptions()[find_idx_by_name(name)];
    }

    const types::field_description& schema::get_field_description(components::catalog::field_id_t id) const {
        return to_struct(schema_struct).descriptions()[find_idx_by_id(id)];
    }

    const types::complex_logical_type& schema::find_field(field_id_t id) const {
        return schema_struct.child_types()[find_idx_by_id(id)];
    }

    const std::pmr::vector<field_id_t>& schema::primary_key() const { return primary_key_field_ids; }

    const std::vector<types::complex_logical_type>& schema::columns() const { return schema_struct.child_types(); }

    const std::vector<types::field_description>& schema::descriptions() const {
        return to_struct(schema_struct).descriptions();
    }

    field_id_t schema::highest_field_id() const { return highest; }

    size_t schema::find_idx_by_id(field_id_t id) const {
        if (auto it = id_to_struct_idx.find(id); it != id_to_struct_idx.end()) {
            return it->second;
        }

        throw schema_exception("No field with such id: " + std::to_string(id));
    }

    size_t schema::find_idx_by_name(const std::pmr::string& name) const {
        const auto& fields = schema_struct.child_types();

        auto it = std::find_if(fields.cbegin(), fields.cend(), [&name](const complex_logical_type& type) -> bool {
            return type.alias() == name.c_str();
        });

        if (it != fields.cend()) {
            return static_cast<size_t>(it - fields.cbegin());
        }

        throw schema_exception("No field with such name: \"" + std::string(name) + "\"");
    }
} // namespace components::catalog
