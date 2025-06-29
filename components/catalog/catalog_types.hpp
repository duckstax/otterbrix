#pragma once

#include <cassert>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <components/types/types.hpp>

namespace components::catalog {
    using timestamp = std::chrono::milliseconds;
    using schema_version_t = uint64_t;
    using field_id_t = uint64_t;

    [[nodiscard]] inline types::complex_logical_type
    create_list(field_id_t field_id, const types::complex_logical_type& type, bool required = true) {
        return types::complex_logical_type(
            types::logical_type::LIST,
            std::make_unique<types::detailed_list_logical_type_extention>(field_id, type, required));
    }

    [[nodiscard]] inline types::complex_logical_type create_struct(std::vector<types::complex_logical_type> columns,
                                                                   std::vector<types::field_description> descriptions) {
        return types::complex_logical_type(
            types::logical_type::STRUCT,
            std::make_unique<types::detailed_struct_logical_type_extention>(columns, descriptions));
    }

    [[nodiscard]] inline types::complex_logical_type create_map(field_id_t key_id,
                                                                const types::complex_logical_type& key,
                                                                field_id_t value_id,
                                                                const types::complex_logical_type& value,
                                                                bool value_required = true) {
        return types::complex_logical_type(
            types::logical_type::MAP,
            std::make_unique<types::detailed_map_logical_type_extention>(key_id, key, value_id, value, value_required));
    }

    inline types::detailed_map_logical_type_extention& to_detailed_map(const types::complex_logical_type& type) {
        assert(type.type() == types::logical_type::MAP);
        assert(type.extention()->type() == types::logical_type_extention::extention_type::DETAILED_MAP);
        return static_cast<types::detailed_map_logical_type_extention&>(*type.extention());
    }

    inline types::detailed_struct_logical_type_extention& to_detailed_struct(const types::complex_logical_type& type) {
        assert(type.type() == types::logical_type::STRUCT);
        assert(type.extention()->type() == types::logical_type_extention::extention_type::DETAILED_STRUCT);
        return static_cast<types::detailed_struct_logical_type_extention&>(*type.extention());
    }

    inline types::detailed_list_logical_type_extention& to_detailed_list(const types::complex_logical_type& type) {
        assert(type.type() == types::logical_type::LIST);
        assert(type.extention()->type() == types::logical_type_extention::extention_type::DETAILED_LIST);
        return static_cast<types::detailed_list_logical_type_extention&>(*type.extention());
    }
} // namespace components::catalog
