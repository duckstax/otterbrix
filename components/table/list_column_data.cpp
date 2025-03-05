#include "list_column_data.hpp"

namespace components::table {

    list_column_data_t::list_column_data_t(std::pmr::memory_resource* resource,
                                           storage::block_manager_t& block_manager,
                                           uint64_t column_index,
                                           uint64_t start_row,
                                           types::complex_logical_type type,
                                           column_data_t* parent)
        : column_data_t(resource, block_manager, column_index, start_row, std::move(type), parent)
        , child_column(create_column(resource, block_manager, 1, start_row, type_.child_type(), this))
        , validity(resource, block_manager, 0, start_row, *this) {
        assert(type_.to_physical_type() == types::physical_type::LIST);
    }

    void list_column_data_t::set_start(uint64_t new_start) {
        column_data_t::set_start(new_start);
        child_column->set_start(new_start);
        validity.set_start(new_start);
    }

    filter_propagate_result_t list_column_data_t::check_zonemap(column_scan_state& state, table_filter_t& filter) {
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    void list_column_data_t::initialize_scan(column_scan_state& state) {
        column_data_t::initialize_scan(state);

        assert(state.child_states.size() == 2);
        validity.initialize_scan(state.child_states[0]);

        child_column->initialize_scan(state.child_states[1]);
    }

    uint64_t list_column_data_t::fetch_list_offset(uint64_t row_idx) {
        auto segment = data_.get_segment(row_idx);
        column_fetch_state fetch_state;
        vector::vector_t result(resource_, type_, 1);
        segment->fetch_row(fetch_state, row_idx, result, 0U);

        return result.data<uint64_t>()[0];
    }

    void list_column_data_t::initialize_scan_with_offset(column_scan_state& state, uint64_t row_idx) {
        if (row_idx == 0) {
            initialize_scan(state);
            return;
        }
        column_data_t::initialize_scan_with_offset(state, row_idx);

        assert(state.child_states.size() == 2);
        validity.initialize_scan_with_offset(state.child_states[0], row_idx);

        auto child_offset = row_idx == start_ ? 0 : fetch_list_offset(row_idx - 1);
        assert(child_offset <= child_column->max_entry());
        if (child_offset < child_column->max_entry()) {
            child_column->initialize_scan_with_offset(state.child_states[1], start_ + child_offset);
        }
        state.last_offset = child_offset;
    }

    uint64_t list_column_data_t::scan(uint64_t vector_index,
                                      column_scan_state& state,
                                      vector::vector_t& result,
                                      uint64_t count) {
        return scan_count(state, result, count);
    }

    uint64_t list_column_data_t::scan_committed(uint64_t vector_index,
                                                column_scan_state& state,
                                                vector::vector_t& result,
                                                bool allow_updates,
                                                uint64_t count) {
        return scan_count(state, result, count);
    }

    uint64_t list_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        if (count == 0) {
            return 0;
        }
        assert(!updates_);

        vector::vector_t offset_vector(result.resource(), types::logical_type::UBIGINT, count);
        uint64_t scan_count = scan_vector(state, offset_vector, count, scan_vector_type::SCAN_FLAT_VECTOR);
        assert(scan_count > 0);
        validity.scan_count(state.child_states[0], result, count);

        vector::unified_vector_format offsets(result.resource(), result.size());
        offset_vector.to_unified_format(scan_count, offsets);
        auto data = offsets.get_data<uint64_t>();
        auto last_entry = data[offsets.referenced_indexing->get_index(scan_count - 1)];

        auto result_data = result.data<types::list_entry_t>();
        auto base_offset = state.last_offset;
        uint64_t current_offset = 0;
        for (uint64_t i = 0; i < scan_count; i++) {
            auto offset_index = offsets.referenced_indexing->get_index(i);
            result_data[i].offset = current_offset;
            result_data[i].length = data[offset_index] - current_offset - base_offset;
            current_offset += result_data[i].length;
        }

        assert(last_entry >= base_offset);
        uint64_t child_scan_count = last_entry - base_offset;
        result.reserve(child_scan_count);

        if (child_scan_count > 0) {
            auto& child_entry = result.entry();
            if (child_entry.type().to_physical_type() != types::physical_type::STRUCT &&
                child_entry.type().to_physical_type() != types::physical_type::ARRAY &&
                state.child_states[1].row_index + child_scan_count >
                    child_column->start() + child_column->max_entry()) {
                throw std::runtime_error("list_column_data_t::scan_count - internal list scan offset is out of range");
            }
            child_column->scan_count(state.child_states[1], child_entry, child_scan_count);
        }
        state.last_offset = last_entry;

        result.set_list_size(child_scan_count);
        return scan_count;
    }

    void list_column_data_t::skip(column_scan_state& state, uint64_t count) {
        validity.skip(state.child_states[0], count);

        vector::vector_t offset_vector(resource_, types::logical_type::UBIGINT, count);
        uint64_t scan_count = scan_vector(state, offset_vector, count, scan_vector_type::SCAN_FLAT_VECTOR);
        assert(scan_count > 0);

        vector::unified_vector_format offsets(resource_, count);
        offset_vector.to_unified_format(scan_count, offsets);
        auto data = offsets.get_data<uint64_t>();
        auto last_entry = data[offsets.referenced_indexing->get_index(scan_count - 1)];
        uint64_t child_scan_count = last_entry - state.last_offset;
        if (child_scan_count == 0) {
            return;
        }
        state.last_offset = last_entry;

        child_column->skip(state.child_states[1], child_scan_count);
    }

    void list_column_data_t::initialize_append(column_append_state& state) {
        column_data_t::initialize_append(state);

        column_append_state validity_append_state;
        validity.initialize_append(validity_append_state);
        state.child_appends.push_back(std::move(validity_append_state));

        column_append_state child_append_state;
        child_column->initialize_append(child_append_state);
        state.child_appends.push_back(std::move(child_append_state));
    }

    void list_column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        assert(count > 0);
        vector::unified_vector_format list_data(vector.resource(), count);
        vector.to_unified_format(count, list_data);
        auto& list_validity = list_data.validity;

        auto input_offsets = list_data.get_data<types::list_entry_t>();
        auto start_offset = child_column->max_entry();
        uint64_t child_count = 0;

        vector::validity_mask_t append_mask(resource_, count);
        auto append_offsets = std::unique_ptr<uint64_t[]>(new uint64_t[count]);
        bool child_contiguous = true;
        for (uint64_t i = 0; i < count; i++) {
            auto input_idx = list_data.referenced_indexing->get_index(i);
            if (list_validity.row_is_valid(input_idx)) {
                auto& input_list = input_offsets[input_idx];
                if (input_list.offset != child_count) {
                    child_contiguous = false;
                }
                append_offsets[i] = start_offset + child_count + input_list.length;
                child_count += input_list.length;
            } else {
                append_mask.set_invalid(i);
                append_offsets[i] = start_offset + child_count;
            }
        }
        auto& list_child = vector.entry();
        vector::vector_t child_vector(list_child);
        if (!child_contiguous) {
            vector::indexing_vector_t child_indexing(resource_, child_count);
            uint64_t current_count = 0;
            for (uint64_t i = 0; i < count; i++) {
                auto input_idx = list_data.referenced_indexing->get_index(i);
                if (list_validity.row_is_valid(input_idx)) {
                    auto& input_list = input_offsets[input_idx];
                    for (uint64_t list_idx = 0; list_idx < input_list.length; list_idx++) {
                        child_indexing.set_index(current_count++, input_list.offset + list_idx);
                    }
                }
            }
            assert(current_count == child_count);
            child_vector.slice(list_child, child_indexing, child_count);
        }

        vector::unified_vector_format uvf(resource_, vector.size());
        uvf.referenced_indexing = vector::incremental_indexing_vector();
        uvf.data = reinterpret_cast<std::byte*>(append_offsets.get());

        if (child_count > 0) {
            child_column->append(state.child_appends[1], child_vector, child_count);
        }
        column_data_t::append_data(state, uvf, count);
        uvf.validity = append_mask;
        validity.append_data(state.child_appends[0], uvf, count);
    }

    void list_column_data_t::revert_append(int64_t start_row) {
        column_data_t::revert_append(start_row);
        validity.revert_append(start_row);
        auto column_count = max_entry();
        if (column_count > start_) {
            auto list_offset = fetch_list_offset(column_count - 1);
            child_column->revert_append(list_offset);
        }
    }

    uint64_t list_column_data_t::fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) {
        throw std::logic_error("Function is not implemented: List fetch");
    }

    void list_column_data_t::update(uint64_t column_index,
                                    vector::vector_t& update_vector,
                                    int64_t* row_ids,
                                    uint64_t update_count) {
        throw std::logic_error("Function is not implemented: List update is not supported.");
    }

    void list_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                           vector::vector_t& update_vector,
                                           int64_t* row_ids,
                                           uint64_t update_count,
                                           uint64_t depth) {
        throw std::logic_error("Function is not implemented: List update Column is not supported");
    }

    void list_column_data_t::fetch_row(column_fetch_state& state,
                                       int64_t row_id,
                                       vector::vector_t& result,
                                       uint64_t result_idx) {
        if (state.child_states.empty()) {
            auto child_state = std::make_unique<column_fetch_state>();
            state.child_states.push_back(std::move(child_state));
        }

        auto start_offset = uint64_t(row_id) == start_ ? 0 : fetch_list_offset(static_cast<uint64_t>(row_id - 1));
        auto end_offset = fetch_list_offset(static_cast<uint64_t>(row_id));
        validity.fetch_row(*state.child_states[0], row_id, result, result_idx);

        auto& validity = result.validity();
        auto list_data = result.data<types::list_entry_t>();
        auto& list_entry = list_data[result_idx];
        list_entry.offset = result.size();
        list_entry.length = end_offset - start_offset;
        if (!validity.row_is_valid(result_idx)) {
            assert(list_entry.length == 0);
            return;
        }

        auto child_scan_count = list_entry.length;
        if (child_scan_count > 0) {
            auto child_state = std::make_unique<column_scan_state>();
            auto& child_type = result.type().child_type();
            vector::vector_t child_scan(result.resource(), child_type, child_scan_count);
            child_state->initialize(child_type);
            child_column->initialize_scan_with_offset(*child_state, start_ + start_offset);
            assert(child_type.to_physical_type() == types::physical_type::STRUCT ||
                   child_state->row_index + child_scan_count - start_ <= child_column->max_entry());
            child_column->scan_count(*child_state, child_scan, child_scan_count);

            result.append(child_scan, child_scan_count);
        }
    }

    void list_column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                     std::vector<uint64_t> col_path,
                                                     std::vector<column_segment_info>& result) {
        column_data_t::get_column_segment_info(row_group_index, col_path, result);
        col_path.push_back(0);
        validity.get_column_segment_info(row_group_index, col_path, result);
        col_path.back() = 1;
        child_column->get_column_segment_info(row_group_index, col_path, result);
    }
} // namespace components::table