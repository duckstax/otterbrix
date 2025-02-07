#include "row_group.hpp"

#include <components/table/storage/buffer_manager.hpp>
#include <vector/data_chunk.hpp>

#include "collection.hpp"
#include "row_version_manager.hpp"

namespace components::table {

    constexpr uint64_t COLUMN_IDENTIFIER_ROW_ID = (uint64_t) -1;

    row_group_t::row_group_t(collection_t* collection, uint64_t start, uint64_t count)
        : segment_base_t(start, count)
        , collection_(collection)
        , allocation_size_(0) {}

    void row_group_t::move_to_collection(collection_t* collection, uint64_t new_start) {
        collection_ = collection;
        start = new_start;
        for (auto& column : columns()) {
            column->set_start(new_start);
        }
    }

    std::vector<std::shared_ptr<column_data_t>>& row_group_t::columns() {
        for (uint64_t c = 0; c < get_column_count(); c++) {
            get_column(c);
        }
        return columns_;
    }

    uint64_t row_group_t::get_column_count() const { return columns_.size(); }

    uint64_t row_group_t::row_group_size() const { return collection_->row_group_size(); }

    column_data_t& row_group_t::get_column(const storage_index_t& c) { return get_column(c.primary_index()); }

    column_data_t& row_group_t::get_column(uint64_t c) {
        assert(c < columns_.size());
        if (!is_loaded_) {
            assert(columns_[c]);
            return *columns_[c];
        }
        if (is_loaded_[c]) {
            assert(columns_[c]);
            return *columns_[c];
        }
        std::lock_guard l(row_group_lock_);
        if (columns_[c]) {
            assert(is_loaded_[c]);
            return *columns_[c];
        }
        if (column_pointers_.size() != columns_.size()) {
            throw std::logic_error("Lazy loading a column but the pointer was not set");
        }
        assert(false);
    }

    storage::block_manager_t& row_group_t::block_manager() { return collection_->block_manager(); }

    void row_group_t::initialize_empty(const std::vector<types::complex_logical_type>& types) {
        assert(columns_.empty());
        for (uint64_t i = 0; i < types.size(); i++) {
            auto column_data =
                column_data_t::create_column(collection_->resource(), block_manager(), i, start, types[i]);
            columns_.push_back(std::move(column_data));
        }
    }

    bool row_group_t::initialize_scan_with_offset(collection_scan_state& state, uint64_t vector_offset) {
        auto& column_ids = state.column_ids();
        state.row_group = this;
        state.vector_index = vector_offset;
        state.max_row_group_row = start > state.max_row ? 0 : std::min<uint64_t>(count, state.max_row - start);
        auto row_number = start + vector_offset * vector::DEFAULT_VECTOR_CAPACITY;
        if (state.max_row_group_row == 0) {
            return false;
        }
        assert(!state.column_scans.empty());
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (!column.is_row_id_column()) {
                auto& column_data = get_column(column);
                column_data.initialize_scan_with_offset(state.column_scans[i], row_number);
            } else {
                state.column_scans[i].current = nullptr;
            }
        }
        return true;
    }

    bool row_group_t::initialize_scan(collection_scan_state& state) {
        auto& column_ids = state.column_ids();
        state.row_group = this;
        state.vector_index = 0;
        state.max_row_group_row = start > state.max_row ? 0 : std::min<uint64_t>(count, state.max_row - start);
        if (state.max_row_group_row == 0) {
            return false;
        }
        assert(!state.column_scans.empty());
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            auto column = column_ids[i];
            if (!column.is_row_id_column()) {
                auto& column_data = get_column(column);
                column_data.initialize_scan(state.column_scans[i]);
            } else {
                state.column_scans[i].current = nullptr;
            }
        }
        return true;
    }

    std::unique_ptr<row_group_t> row_group_t::add_column(collection_t* new_collection,
                                                         column_definition_t& new_column,
                                                         const types::logical_value_t& default_value,
                                                         vector::vector_t& result) {
        auto added_column = column_data_t::create_column(collection_->resource(),
                                                         block_manager(),
                                                         get_column_count(),
                                                         start,
                                                         new_column.type());

        uint64_t rows_to_write = count;
        if (rows_to_write > 0) {
            column_append_state state;
            added_column->initialize_append(state);
            for (uint64_t i = 0; i < rows_to_write; i += vector::DEFAULT_VECTOR_CAPACITY) {
                uint64_t rows_in_this_vector = std::min<uint64_t>(rows_to_write - i, vector::DEFAULT_VECTOR_CAPACITY);
                assert(result.type() == default_value.type());
                result.reference(default_value);
                added_column->append(state, result, rows_in_this_vector);
            }
        }

        auto row_group = std::make_unique<row_group_t>(new_collection, start, count);
        row_group->set_version_info(get_or_create_version_info_ptr());
        row_group->current_version_ = current_version_;
        row_group->columns_ = columns();
        row_group->columns_.push_back(std::move(added_column));

        return row_group;
    }

    std::unique_ptr<row_group_t> row_group_t::remove_column(collection_t* new_collection, uint64_t removed_column) {
        assert(removed_column < columns_.size());

        auto row_group = std::make_unique<row_group_t>(new_collection, start, count);
        row_group->set_version_info(get_or_create_version_info_ptr());
        row_group->current_version_ = current_version_;
        auto& cols = columns();
        for (uint64_t i = 0; i < cols.size(); i++) {
            if (i != removed_column) {
                row_group->columns_.push_back(cols[i]);
            }
        }

        return row_group;
    }

    void row_group_t::next_vector(collection_scan_state& state) {
        state.vector_index++;
        const auto& column_ids = state.column_ids();
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            const auto& column = column_ids[i];
            if (column.is_row_id_column()) {
                continue;
            }
            get_column(column).skip(state.column_scans[i]);
        }
    }

    bool row_group_t::check_zonemap_segments(collection_scan_state& state) {
        auto& filters = state.filter_info();
        for (auto& entry : filters.filter_list()) {
            if (entry.always_true) {
                continue;
            }
            auto column_idx = entry.scan_column_index;
            auto base_column_idx = entry.table_column_index;
            auto& filter = entry.filter;

            auto prune_result = get_column(base_column_idx).check_zonemap(state.column_scans[column_idx], filter);
            if (prune_result != filter_propagate_result_t::ALWAYS_FALSE) {
                continue;
            }

            uint64_t target_row =
                state.column_scans[column_idx].current->start + state.column_scans[column_idx].current->count;
            if (target_row >= state.max_row) {
                target_row = state.max_row;
            }

            assert(target_row >= start);
            assert(target_row <= start + count);
            uint64_t target_vector_index = (target_row - start) / vector::DEFAULT_VECTOR_CAPACITY;
            if (state.vector_index == target_vector_index) {
                return true;
            }
            while (state.vector_index < target_vector_index) {
                next_vector(state);
            }
            return false;
        }

        return true;
    }

    template<table_scan_type TYPE>
    void row_group_t::templated_scan(collection_scan_state& state, vector::data_chunk_t& result) {
        const bool ALLOW_UPDATES = TYPE != table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES &&
                                   TYPE != table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED;
        const auto& column_ids = state.column_ids();
        auto& filter_info = state.filter_info();
        while (true) {
            if (state.vector_index * vector::DEFAULT_VECTOR_CAPACITY >= state.max_row_group_row) {
                return;
            }
            uint64_t current_row = state.vector_index * vector::DEFAULT_VECTOR_CAPACITY;
            auto max_count = std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, state.max_row_group_row - current_row);

            if (!check_zonemap_segments(state)) {
                continue;
            }

            uint64_t count;
            if (TYPE == table_scan_type::REGULAR) {
                count = state.row_group->indexing_vector(state.vector_index, state.valid_indexing, max_count);
                if (count == 0) {
                    next_vector(state);
                    continue;
                }
            } else if (TYPE == table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) {
                count = state.row_group->commited_indexing_vector(state.vector_index, state.valid_indexing, max_count);
                if (count == 0) {
                    next_vector(state);
                    continue;
                }
            } else {
                count = max_count;
            }

            bool has_filters = filter_info.has_filters();
            if (count == max_count && !has_filters) {
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    const auto& column = column_ids[i];
                    if (column.is_row_id_column()) {
                        assert(result.data[i].type().type() == types::logical_type::BIGINT);
                        result.data[i].sequence(static_cast<int64_t>(start + current_row), 1, count);
                    } else {
                        auto& col_data = get_column(column);
                        if (TYPE != table_scan_type::REGULAR) {
                            col_data.scan_committed(state.vector_index,
                                                    state.column_scans[i],
                                                    result.data[i],
                                                    ALLOW_UPDATES);
                        } else {
                            col_data.scan(state.vector_index, state.column_scans[i], result.data[i]);
                        }
                    }
                }
            } else {
                uint64_t approved_tuple_count = count;
                vector::indexing_vector_t indexing;
                if (count != max_count) {
                    indexing = state.valid_indexing;
                } else {
                    indexing.reset(nullptr);
                }
                auto adaptive_filter = filter_info.adaptive_filter();
                auto filter_state = filter_info.begin_filter();
                if (has_filters) {
                    assert(ALLOW_UPDATES);
                    auto& filter_list = filter_info.filter_list();
                    for (uint64_t i = 0; i < filter_list.size(); i++) {
                        auto filter_idx = adaptive_filter->permutation[i];
                        auto& filter = filter_list[filter_idx];
                        if (filter.always_true) {
                            continue;
                        }

                        const auto scan_idx = filter.scan_column_index;
                        const auto column_idx = filter.table_column_index;

                        if (column_idx == COLUMN_IDENTIFIER_ROW_ID) {
                            assert(result.data[i].type().type() == types::logical_type::BIGINT);
                            result.data[i].set_vector_type(vector::vector_type::FLAT);
                            auto result_data = result.data[i].data<int64_t>();
                            for (size_t indexing_idx = 0; indexing_idx < approved_tuple_count; indexing_idx++) {
                                result_data[indexing.get_index(indexing_idx)] =
                                    static_cast<int64_t>(start + current_row + indexing.get_index(indexing_idx));
                            }

                            vector::unified_vector_format vdata(collection_->resource(), result.size());
                            result.data[i].to_unified_format(approved_tuple_count, vdata);
                            column_segment_t::filter_indexing(indexing,
                                                              result.data[i],
                                                              vdata,
                                                              filter.filter,
                                                              approved_tuple_count,
                                                              approved_tuple_count);
                        } else {
                            auto& col_data = get_column(filter.table_column_index);
                            col_data.filter(state.vector_index,
                                            state.column_scans[scan_idx],
                                            result.data[scan_idx],
                                            indexing,
                                            approved_tuple_count,
                                            filter.filter);
                        }
                    }
                    for (auto& table_filter : filter_list) {
                        if (table_filter.always_true) {
                            continue;
                        }
                        result.data[table_filter.scan_column_index].slice(indexing, approved_tuple_count);
                    }
                }
                if (approved_tuple_count == 0) {
                    assert(has_filters);
                    result.reset();
                    for (uint64_t i = 0; i < column_ids.size(); i++) {
                        auto& col_idx = column_ids[i];
                        if (col_idx.is_row_id_column()) {
                            continue;
                        }
                        if (has_filters && filter_info.column_has_filters(i)) {
                            continue;
                        }
                        auto& col_data = get_column(col_idx);
                        col_data.skip(state.column_scans[i]);
                    }
                    state.vector_index++;
                    continue;
                }
                for (uint64_t i = 0; i < column_ids.size(); i++) {
                    if (has_filters && filter_info.column_has_filters(i)) {
                        continue;
                    }
                    auto& column = column_ids[i];
                    if (column.is_row_id_column()) {
                        assert(result.data[i].type().type() == types::logical_type::BIGINT);
                        result.data[i].set_vector_type(vector::vector_type::FLAT);
                        auto result_data = result.data[i].data<int64_t>();
                        for (size_t indexing_idx = 0; indexing_idx < approved_tuple_count; indexing_idx++) {
                            result_data[indexing_idx] =
                                static_cast<int64_t>(start + current_row + indexing.get_index(indexing_idx));
                        }
                    } else {
                        auto& col_data = get_column(column);
                        if (TYPE == table_scan_type::REGULAR) {
                            col_data.select(state.vector_index,
                                            state.column_scans[i],
                                            result.data[i],
                                            indexing,
                                            approved_tuple_count);
                        } else {
                            col_data.select_committed(state.vector_index,
                                                      state.column_scans[i],
                                                      result.data[i],
                                                      indexing,
                                                      approved_tuple_count,
                                                      ALLOW_UPDATES);
                        }
                    }
                }
                filter_info.end_filter(filter_state);

                assert(approved_tuple_count > 0);
                count = approved_tuple_count;
            }
            result.set_cardinality(count);
            state.vector_index++;
            break;
        }
    }

    void row_group_t::scan(collection_scan_state& state, vector::data_chunk_t& result) {
        templated_scan<table_scan_type::REGULAR>(state, result);
    }

    void row_group_t::scan_committed(collection_scan_state& state, vector::data_chunk_t& result, table_scan_type type) {
        switch (type) {
            case table_scan_type::COMMITTED_ROWS:
                templated_scan<table_scan_type::COMMITTED_ROWS>(state, result);
                break;
            case table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES:
                templated_scan<table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES>(state, result);
                break;
            case table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED:
            case table_scan_type::LATEST_COMMITTED_ROWS:
                templated_scan<table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED>(state, result);
                break;
            default:
                throw std::logic_error("Unrecognized table scan type");
        }
    }

    void row_group_t::fetch_row(column_fetch_state& state,
                                const std::vector<storage_index_t>& column_ids,
                                int64_t row_id,
                                vector::data_chunk_t& result,
                                uint64_t result_idx) {
        for (uint64_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
            auto& column = column_ids[col_idx];
            auto& result_vector = result.data[col_idx];
            assert(result_vector.get_vector_type() == vector::vector_type::FLAT);
            assert(!result_vector.is_null(result_idx));
            if (column.is_row_id_column()) {
                assert(result_vector.type().to_physical_type() == types::physical_type::INT64);
                result_vector.set_vector_type(vector::vector_type::FLAT);
                auto data = result_vector.data<int64_t>();
                data[result_idx] = row_id;
            } else {
                auto& col_data = get_column(column);
                col_data.fetch_row(state, row_id, result_vector, result_idx);
            }
        }
    }

    void row_group_t::append_version_info(uint64_t count) {
        uint64_t row_group_start = this->count.load();
        uint64_t row_group_end = row_group_start + count;
        if (row_group_end > row_group_size()) {
            row_group_end = row_group_size();
        }
        this->count = row_group_end;
    }

    void row_group_t::initialize_append(row_group_append_state& append_state) {
        append_state.row_group = this;
        append_state.offset_in_row_group = count;
        append_state.states = std::make_unique<column_append_state[]>(get_column_count());
        for (uint64_t i = 0; i < get_column_count(); i++) {
            auto& col_data = get_column(i);
            col_data.initialize_append(append_state.states[i]);
        }
    }

    void row_group_t::append(row_group_append_state& state, vector::data_chunk_t& chunk, uint64_t append_count) {
        assert(chunk.column_count() == get_column_count());
        for (uint64_t i = 0; i < get_column_count(); i++) {
            auto& col_data = get_column(i);
            auto prev_allocation_size = col_data.allocation_size();
            col_data.append(state.states[i], chunk.data[i], append_count);
            allocation_size_ += col_data.allocation_size() - prev_allocation_size;
        }
        state.offset_in_row_group += append_count;
    }

    void row_group_t::update(vector::data_chunk_t& update_chunk,
                             int64_t* ids,
                             uint64_t offset,
                             uint64_t count,
                             const std::vector<uint64_t>& column_ids) {
        for (uint64_t i = 0; i < column_ids.size(); i++) {
            auto column = column_ids[i];
            assert(column != COLUMN_IDENTIFIER_ROW_ID);
            auto& col_data = get_column(column);
            assert(col_data.type().type() == update_chunk.data[i].type().type());
            if (offset > 0) {
                vector::vector_t sliced_vector(update_chunk.data[i], offset, offset + count);
                sliced_vector.flatten(count);
                col_data.update(column, sliced_vector, ids + offset, count);
            } else {
                col_data.update(column, update_chunk.data[i], ids, count);
            }
        }
    }

    void row_group_t::update_column(vector::data_chunk_t& updates,
                                    vector::vector_t& row_ids,
                                    const std::vector<uint64_t>& column_path) {
        assert(updates.column_count() == 1);
        auto ids = row_ids.data<int64_t>();

        auto primary_column_idx = column_path[0];
        assert(primary_column_idx != COLUMN_IDENTIFIER_ROW_ID);
        assert(primary_column_idx < columns_.size());
        auto& col_data = get_column(primary_column_idx);
        col_data.update_column(column_path, updates.data[0], ids, updates.size(), 1);
    }

    uint64_t row_group_t::committed_row_count() { return count; }

    bool row_group_t::has_unloaded_deletes() const {
        if (deletes_pointers_.empty()) {
            return false;
        }
        return !deletes_is_loaded_;
    }

    void row_group_t::get_column_segment_info(uint64_t row_group_index, std::vector<column_segment_info>& result) {
        for (uint64_t col_idx = 0; col_idx < get_column_count(); col_idx++) {
            auto& col_data = get_column(col_idx);
            col_data.get_column_segment_info(row_group_index, {col_idx}, result);
        }
    }

    class version_delete_state {
    public:
        version_delete_state(row_group_t& info, uint64_t current_version, data_table_t& table, uint64_t base_row)
            : info(info)
            , current_vesrion(current_version)
            , table(table)
            , current_chunk(storage::INVALID_INDEX)
            , count(0)
            , base_row(base_row)
            , delete_count(0) {}

        row_group_t& info;
        data_table_t& table;
        uint64_t current_chunk;
        uint64_t current_vesrion;
        int64_t rows[vector::DEFAULT_VECTOR_CAPACITY];
        uint64_t count;
        uint64_t base_row;
        uint64_t chunk_row;
        uint64_t delete_count;

        void delete_row(int64_t row_id);
        void flush();
    };

    uint64_t row_group_t::delete_rows(data_table_t& table, int64_t* ids, uint64_t count) {
        version_delete_state del_state(*this, current_version_, table, start);

        for (uint64_t i = 0; i < count; i++) {
            assert(ids[i] >= 0);
            assert(uint64_t(ids[i]) >= start && uint64_t(ids[i]) < start + this->count);
            del_state.delete_row(ids[i] - static_cast<int64_t>(start));
        }
        del_state.flush();
        return del_state.delete_count;
    }

    uint64_t row_group_t::delete_rows(uint64_t vector_idx, int64_t rows[], uint64_t count) {
        return get_or_create_version_info().delete_rows(vector_idx, ++current_version_, rows, count);
    }

    row_version_manager_t& row_group_t::get_or_create_version_info() {
        auto vinfo = version_info();
        if (vinfo) {
            return *vinfo;
        }
        return *get_or_create_version_info_internal();
    }

    std::shared_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_ptr() {
        auto vinfo = version_info();
        if (vinfo) {
            return owned_version_info_;
        }
        return get_or_create_version_info_internal();
    }

    uint64_t
    row_group_t::indexing_vector(uint64_t vector_idx, vector::indexing_vector_t& indexing_vector, uint64_t max_count) {
        auto vinfo = version_info();
        if (!vinfo) {
            return max_count;
        }
        return vinfo->indexing_vector({current_version_, current_version_}, vector_idx, indexing_vector, max_count);
    }

    uint64_t row_group_t::commited_indexing_vector(uint64_t vector_idx,
                                                   vector::indexing_vector_t& indexing_vector,
                                                   uint64_t max_count) {
        auto vinfo = version_info();
        if (!vinfo) {
            return max_count;
        }
        return vinfo->commited_indexing_vector(current_version_,
                                               current_version_,
                                               vector_idx,
                                               indexing_vector,
                                               max_count);
    }

    std::shared_ptr<row_version_manager_t> row_group_t::get_or_create_version_info_internal() {
        std::lock_guard lock(row_group_lock_);
        if (!owned_version_info_) {
            auto new_info = std::make_shared<row_version_manager_t>(start);
            set_version_info(std::move(new_info));
        }
        return owned_version_info_;
    }

    row_version_manager_t* row_group_t::version_info() {
        if (!has_unloaded_deletes()) {
            return version_info_;
        }
        std::lock_guard lock(row_group_lock_);
        if (!has_unloaded_deletes()) {
            return version_info_;
        }
        set_version_info(nullptr);
        deletes_is_loaded_ = true;
        return version_info_;
    }

    void row_group_t::set_version_info(std::shared_ptr<row_version_manager_t> version) {
        owned_version_info_ = std::move(version);
        version_info_ = owned_version_info_.get();
    }

    void version_delete_state::delete_row(int64_t row_id) {
        assert(row_id >= 0);
        uint64_t vector_idx = static_cast<uint64_t>(row_id) / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t idx_in_vector = static_cast<uint64_t>(row_id) - vector_idx * vector::DEFAULT_VECTOR_CAPACITY;
        if (current_chunk != vector_idx) {
            flush();

            current_chunk = vector_idx;
            chunk_row = vector_idx * vector::DEFAULT_VECTOR_CAPACITY;
        }
        rows[count++] = static_cast<int64_t>(idx_in_vector);
    }

    void version_delete_state::flush() {
        if (count == 0) {
            return;
        }
        auto actual_delete_count = info.delete_rows(current_chunk, rows, count);
        delete_count += actual_delete_count;
        count = 0;
    }
} // namespace components::table