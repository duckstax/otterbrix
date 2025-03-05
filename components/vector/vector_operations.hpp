#pragma once

#include "vector.hpp"

namespace components::vector::vector_ops {
    void generate_sequence(vector_t& result, uint64_t count, int64_t start, int64_t increment);
    void generate_sequence(vector_t& result,
                           uint64_t count,
                           const indexing_vector_t& indexing,
                           int64_t start,
                           int64_t increment);

    void copy(const vector_t& source,
              vector_t& target,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset);
    void copy(const vector_t& source,
              vector_t& target,
              const indexing_vector_t& indexing,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset);
    void copy(const vector_t& source,
              vector_t& target,
              const indexing_vector_t& indexing,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset,
              uint64_t copy_count);

    void hash(vector_t& input, vector_t& result, uint64_t count);
    void hash(vector_t& input, vector_t& result, const indexing_vector_t& indexing, uint64_t count);

    void combine_hash(vector_t& hashes, vector_t& input, uint64_t count);
    void combine_hash(vector_t& hashes, vector_t& input, const indexing_vector_t& rindexing, uint64_t count);
    void write_to_storage(vector_t& source, uint64_t count, std::byte* target);
} // namespace components::vector::vector_ops