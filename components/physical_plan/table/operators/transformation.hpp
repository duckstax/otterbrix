#pragma once
#include <components/vector/data_chunk.hpp>

namespace components::document {
    class value_t;
}

namespace services::table::operators::impl {

    using value_matrix_t = std::pmr::vector<std::pmr::vector<components::types::logical_value_t>>;

    static value_matrix_t transpose(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk);
    static components::vector::data_chunk_t
    transpose(std::pmr::memory_resource* resource,
              const value_matrix_t& matrix,
              const std::vector<components::types::complex_logical_type>& types);

    components::types::logical_value_t convert(const components::document::value_t& value);

} // namespace services::table::operators::impl