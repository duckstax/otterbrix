#include "transformation.hpp"

#include <value.hpp>

namespace services::table::operators::impl {

    value_matrix_t transpose(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk) {
        std::pmr::vector<std::pmr::vector<components::types::logical_value_t>> matrix(resource);
        matrix.reserve(chunk.size());
        for (size_t i = 0; i < matrix.size(); i++) {
            matrix[i].emplace_back(resource);
            matrix[i].reserve(chunk.column_count());
        }
        for (size_t i = 0; i < chunk.size(); i++) {
            for (size_t j = 0; j < chunk.column_count(); j++) {
                matrix[i][j] = chunk.value(j, i);
            }
        }

        return matrix;
    }

    components::vector::data_chunk_t transpose(std::pmr::memory_resource* resource,
                                               const value_matrix_t& matrix,
                                               const std::vector<components::types::complex_logical_type>& types) {
        auto chunk = components::vector::data_chunk_t(resource, types, matrix.size());
        for (size_t i = 0; i < chunk.size(); i++) {
            for (size_t j = 0; j < chunk.column_count(); j++) {
                chunk.set_value(j, i, matrix[i][j]);
            }
        }
        return chunk;
    }

    components::types::logical_value_t convert(const components::document::value_t& value) {
        switch (value.physical_type()) {
            case components::types::physical_type::BOOL:
                return components::types::logical_value_t(value.as<bool>());
            case components::types::physical_type::UINT8:
                return components::types::logical_value_t(value.as<uint8_t>());
            case components::types::physical_type::INT8:
                return components::types::logical_value_t(value.as<int8_t>());
            case components::types::physical_type::UINT16:
                return components::types::logical_value_t(value.as<uint16_t>());
            case components::types::physical_type::INT16:
                return components::types::logical_value_t(value.as<int16_t>());
            case components::types::physical_type::UINT32:
                return components::types::logical_value_t(value.as<uint32_t>());
            case components::types::physical_type::INT32:
                return components::types::logical_value_t(value.as<int32_t>());
            case components::types::physical_type::UINT64:
                return components::types::logical_value_t(value.as<uint64_t>());
            case components::types::physical_type::INT64:
                return components::types::logical_value_t(value.as<int64_t>());
            // case components::types::physical_type::UINT128:
            //     return components::types::logical_value_t(value.as<uint128_t>());
            // case components::types::physical_type::INT128:
            //     return components::types::logical_value_t(value.as<int128_t>());
            case components::types::physical_type::FLOAT:
                return components::types::logical_value_t(value.as<float>());
            case components::types::physical_type::DOUBLE:
                return components::types::logical_value_t(value.as<double>());
            case components::types::physical_type::STRING:
                return components::types::logical_value_t(value.as_string());
            case components::types::physical_type::NA:
                return components::types::logical_value_t(nullptr);
            default:
                throw std::runtime_error("unsupported type in value conversion");
        }
    }

} // namespace services::table::operators::impl