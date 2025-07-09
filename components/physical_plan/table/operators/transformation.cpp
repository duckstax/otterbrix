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

} // namespace services::table::operators::impl