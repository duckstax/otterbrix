#pragma once

#include "catalog_error.hpp"
#include "catalog_types.hpp"

#include <algorithm>
#include <memory_resource>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace components::catalog {
    class schema {
    public:
        explicit schema(std::pmr::memory_resource* resource,
                        const components::types::complex_logical_type& schema_struct,
                        const std::pmr::vector<field_id_t>& primary_key = {});

        const types::complex_logical_type& find_field(field_id_t id) const;
        const types::complex_logical_type& find_field(const std::pmr::string& name) const;

        [[nodiscard]] const types::field_description& get_field_description(field_id_t id) const;
        [[nodiscard]] const types::field_description& get_field_description(const std::pmr::string& name) const;

        [[nodiscard]] const std::pmr::vector<field_id_t>& primary_key() const;
        [[nodiscard]] const std::vector<types::complex_logical_type>& columns() const;
        [[nodiscard]] const std::vector<types::field_description>& descriptions() const;
        [[nodiscard]] field_id_t highest_field_id() const;

        [[nodiscard]] const catalog_error& error() const;

    private:
        size_t find_idx_by_id(field_id_t id) const;
        size_t find_idx_by_name(const std::pmr::string& name) const;

        components::types::complex_logical_type schema_struct;
        std::pmr::vector<field_id_t> primary_key_field_ids;
        std::pmr::unordered_map<field_id_t, size_t> id_to_struct_idx;
        field_id_t highest = 0;
        mutable catalog_error error_;
    };
} // namespace components::catalog
