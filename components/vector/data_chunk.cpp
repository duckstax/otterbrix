#include "data_chunk.hpp"
#include "vector_operations.hpp"
#include <stdexcept>

namespace components::vector {

    data_chunk_t::data_chunk_t(std::pmr::memory_resource* resource,
                               const std::vector<types::complex_logical_type>& types,
                               uint64_t capacity) {
        capacity_ = capacity;
        for (uint64_t i = 0; i < types.size(); i++) {
            data.emplace_back(resource, types[i], capacity_);
        }
    }

    uint64_t data_chunk_t::allocation_size() const {
        uint64_t total_size = 0;
        auto cardinality = size();
        for (auto& vec : data) {
            total_size += vec.allocation_size(cardinality);
        }
        return total_size;
    }

    void data_chunk_t::reset() {
        if (data.empty()) {
            return;
        }
        capacity_ = DEFAULT_VECTOR_CAPACITY;
        set_cardinality(0);
    }

    void data_chunk_t::destroy() {
        data.clear();
        capacity_ = 0;
        set_cardinality(0);
    }

    types::logical_value_t data_chunk_t::value(uint64_t col_idx, uint64_t index) const {
        assert(index < size());
        return data[col_idx].value(index);
    }

    void data_chunk_t::set_value(uint64_t col_idx, uint64_t index, const types::logical_value_t& val) {
        data[col_idx].set_value(index, val);
    }

    bool data_chunk_t::all_constant() const {
        for (auto& v : data) {
            if (v.get_vector_type() != vector_type::CONSTANT) {
                return false;
            }
        }
        return true;
    }

    void data_chunk_t::reference(data_chunk_t& chunk) {
        assert(chunk.column_count() <= column_count());
        set_capacity(chunk);
        set_cardinality(chunk);
        for (uint64_t i = 0; i < chunk.column_count(); i++) {
            data[i].reference(chunk.data[i]);
        }
    }

    void data_chunk_t::copy(data_chunk_t& other, uint64_t offset) const {
        assert(column_count() == other.column_count());
        assert(other.size() == 0);

        for (uint64_t i = 0; i < column_count(); i++) {
            assert(other.data[i].get_vector_type() == vector_type::FLAT);
            vector_ops::copy(data[i], other.data[i], size(), offset, 0);
        }
        other.set_cardinality(size() - offset);
    }

    void data_chunk_t::copy(data_chunk_t& other,
                            const indexing_vector_t& indexing,
                            uint64_t source_count,
                            uint64_t offset) const {
        assert(column_count() == other.column_count());
        assert(other.size() == 0);
        assert(source_count <= size());

        for (uint64_t i = 0; i < column_count(); i++) {
            assert(other.data[i].get_vector_type() == vector_type::FLAT);
            vector_ops::copy(data[i], other.data[i], indexing, source_count, offset, 0);
        }
        other.set_cardinality(source_count - offset);
    }

    void data_chunk_t::split(data_chunk_t& other, uint64_t split_idx) {
        assert(other.size() == 0);
        assert(other.data.empty());
        assert(split_idx < data.size());
        uint64_t num_cols = data.size();
        for (uint64_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
            other.data.push_back(std::move(data[col_idx]));
        }
        for (uint64_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
            data.pop_back();
        }
        other.set_capacity(*this);
        other.set_cardinality(*this);
    }

    void data_chunk_t::fuse(data_chunk_t&& other) {
        assert(other.size() == size());
        uint64_t num_cols = other.data.size();
        for (uint64_t col_idx = 0; col_idx < num_cols; ++col_idx) {
            data.emplace_back(std::move(other.data[col_idx]));
        }
        other.destroy();
    }

    void data_chunk_t::reference_columns(data_chunk_t& other, const std::vector<uint64_t>& column_ids) {
        assert(column_count() == column_ids.size());
        reset();
        for (uint64_t col_idx = 0; col_idx < column_count(); col_idx++) {
            auto& other_col = other.data[column_ids[col_idx]];
            auto& this_col = data[col_idx];
            assert(other_col.type() == this_col.type());
            this_col.reference(other_col);
        }
        set_cardinality(other.size());
    }

    void
    data_chunk_t::append(const data_chunk_t& other, bool resize, indexing_vector_t* indexing, uint64_t indexing_count) {
        uint64_t new_size = indexing ? size() + indexing_count : size() + other.size();
        if (other.size() == 0) {
            return;
        }
        if (column_count() != other.column_count()) {
            throw std::logic_error("Column counts of appending chunk doesn't match!");
        }
        if (new_size > capacity_) {
            if (resize) {
                auto new_capacity = next_power_of_two(new_size);
                for (uint64_t i = 0; i < column_count(); i++) {
                    data[i].resize(size(), new_capacity);
                }
                capacity_ = new_capacity;
            } else {
                throw std::logic_error("Can't append chunk to other chunk without resizing");
            }
        }
        for (uint64_t i = 0; i < column_count(); i++) {
            assert(data[i].get_vector_type() == vector_type::FLAT);
            if (indexing) {
                vector_ops::copy(other.data[i], data[i], *indexing, indexing_count, 0, size());
            } else {
                vector_ops::copy(other.data[i], data[i], other.size(), 0, size());
            }
        }
        set_cardinality(new_size);
    }

    void data_chunk_t::flatten() {
        for (uint64_t i = 0; i < column_count(); i++) {
            data[i].flatten(size());
        }
    }

    std::vector<types::complex_logical_type> data_chunk_t::types() const {
        std::vector<types::complex_logical_type> types;
        for (uint64_t i = 0; i < column_count(); i++) {
            types.push_back(data[i].type());
        }
        return types;
    }

    void data_chunk_t::slice(const indexing_vector_t& indexing_vector, uint64_t count) {
        count_ = count;
        indexing_cache_t merge_cache;
        for (uint64_t c = 0; c < column_count(); c++) {
            data[c].slice(indexing_vector, count, merge_cache);
        }
    }

    void data_chunk_t::slice(const data_chunk_t& other,
                             const indexing_vector_t& indexing,
                             uint64_t count,
                             uint64_t col_offset) {
        assert(other.column_count() <= col_offset + column_count());
        count_ = count;
        indexing_cache_t merge_cache;
        for (uint64_t c = 0; c < other.column_count(); c++) {
            if (other.data[c].get_vector_type() == vector_type::DICTIONARY) {
                // already a dictionary! merge the dictionaries
                data[col_offset + c].reference(other.data[c]);
                data[col_offset + c].slice(indexing, count, merge_cache);
            } else {
                data[col_offset + c].slice(other.data[c], indexing, count);
            }
        }
    }

    void data_chunk_t::slice(std::pmr::memory_resource* resource, uint64_t offset, uint64_t slice_count) {
        assert(offset + slice_count <= size());
        indexing_vector_t indexing(resource, slice_count);
        for (uint64_t i = 0; i < slice_count; i++) {
            indexing.set_index(i, offset + i);
        }
        slice(indexing, slice_count);
    }

    std::vector<unified_vector_format> data_chunk_t::to_unified_format(std::pmr::memory_resource* resource) {
        std::vector<unified_vector_format> unified_data;
        unified_data.reserve(column_count());
        for (uint64_t col_idx = 0; col_idx < column_count(); col_idx++) {
            unified_data.emplace_back(resource, size());
            data[col_idx].to_unified_format(size(), unified_data[col_idx]);
        }
        return unified_data;
    }

    void data_chunk_t::hash(vector_t& result) {
        assert(result.type().type() == types::logical_type::UBIGINT);
        vector_ops::hash(data[0], result, size());
        for (uint64_t i = 1; i < column_count(); i++) {
            vector_ops::combine_hash(result, data[i], size());
        }
    }

    void data_chunk_t::hash(std::vector<uint64_t>& column_ids, vector_t& result) {
        assert(result.type().type() == types::logical_type::UBIGINT);
        assert(!column_ids.empty());

        vector_ops::hash(data[column_ids[0]], result, size());
        for (uint64_t i = 1; i < column_ids.size(); i++) {
            vector_ops::combine_hash(result, data[column_ids[i]], size());
        }
    }

} // namespace components::vector