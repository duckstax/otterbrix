#pragma once
#include "vector.hpp"
#include <components/types/logical_value.hpp>

namespace components::vector {

    class data_chunk_t {
    public:
        data_chunk_t(std::pmr::memory_resource* resource,
                     const std::vector<types::complex_logical_type>& types,
                     uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        data_chunk_t(const data_chunk_t&) = delete;
        data_chunk_t& operator=(const data_chunk_t&) = delete;
        data_chunk_t(data_chunk_t&&) = default;
        data_chunk_t& operator=(data_chunk_t&&) = default;
        ~data_chunk_t() = default;

        std::vector<vector_t> data;

        uint64_t size() const { return count_; }
        uint64_t column_count() const { return data.size(); }
        void set_cardinality(uint64_t count) {
            assert(count <= capacity_);
            count_ = count;
        }
        void set_cardinality(const data_chunk_t& other) { set_cardinality(other.size()); }
        void set_capacity(uint64_t capacity) { capacity_ = capacity; }
        void set_capacity(const data_chunk_t& other) { set_capacity(other.capacity_); }

        types::logical_value_t value(uint64_t col_idx, uint64_t index) const;
        void set_value(uint64_t col_idx, uint64_t index, const types::logical_value_t& val);

        uint64_t allocation_size() const;

        bool all_constant() const;

        void reference(data_chunk_t& chunk);

        void append(const data_chunk_t& other,
                    bool resize = false,
                    indexing_vector_t* indexing = nullptr,
                    uint64_t count = 0);

        void destroy();

        void copy(data_chunk_t& other, uint64_t offset = 0) const;
        void
        copy(data_chunk_t& other, const indexing_vector_t& indexing, uint64_t source_count, uint64_t offset = 0) const;

        void split(data_chunk_t& other, uint64_t split_idx);

        void fuse(data_chunk_t&& other);

        void reference_columns(data_chunk_t& other, const std::vector<uint64_t>& column_ids);

        void flatten();

        std::vector<unified_vector_format> to_unified_format(std::pmr::memory_resource* resource);

        void slice(const indexing_vector_t& indexing_vector, uint64_t count);

        void
        slice(const data_chunk_t& other, const indexing_vector_t& indexing, uint64_t count, uint64_t col_offset = 0);

        void slice(std::pmr::memory_resource* resource, uint64_t offset, uint64_t count);

        void reset();

        void hash(vector_t& result);
        void hash(std::vector<uint64_t>& column_ids, vector_t& result);

        std::vector<types::complex_logical_type> types() const;

    private:
        uint64_t count_ = 0;
        uint64_t capacity_ = DEFAULT_VECTOR_CAPACITY;
    };
} // namespace components::vector