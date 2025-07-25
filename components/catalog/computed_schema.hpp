#pragma once

#include "catalog_exception.hpp"
#include "schema.hpp"
#include "versioned_trie/versioned_trie.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/types/types.hpp>
#include <unordered_map>
#include <unordered_set>

namespace components::catalog {
    class computed_schema {
        using versions = std::pmr::vector<types::complex_logical_type>;

    public:
        explicit computed_schema(std::pmr::memory_resource* resource);

        void append(std::pmr::string json, const components::types::logical_type& type);
        void drop(std::pmr::string json, const components::types::logical_type& type);

        const versions& find_field_versions(field_id_t id) const;
        const versions& find_field_versions(const std::pmr::string& name) const;

        [[nodiscard]] const std::vector<types::complex_logical_type>& latest_columns() const;
        [[nodiscard]] field_id_t highest_field_id() const;

    private:
        versioned_trie<std::pmr::string, components::types::logical_type> fields;
        std::pmr::unordered_map<
            std::pmr::string,
            std::pmr::vector<std::reference_wrapper<versioned_entry<components::types::logical_type>>>>
            existing_versions;
        std::pmr::unordered_map<field_id_t, std::pmr::string> id_to_name;
        field_id_t highest = 0;
    };
} // namespace components::catalog
