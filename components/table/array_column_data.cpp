#include "array_column_data.hpp"
#include "row_group.hpp"
#include <components/vector/vector.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::table {

    array_column_data_t::array_column_data_t(std::pmr::memory_resource* resource,
                                             storage::block_manager_t& block_manager,
                                             uint64_t column_index,
                                             uint64_t start_row,
                                             types::complex_logical_type type,
                                             column_data_t* parent)
        : column_data_t(resource, block_manager, column_index, start_row, std::move(type), parent)
        , child_column(create_column(resource, block_manager, 1, start_row, type_.child_type(), this))
        , validity(resource, block_manager, 0, start_row, *this) {}

    void array_column_data_t::set_start(uint64_t new_start) {
        start_ = new_start;
        child_column->set_start(new_start);
        validity.set_start(new_start);
    }

    filter_propagate_result_t array_column_data_t::check_zonemap(column_scan_state& state, table_filter_t& filter) {
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    void array_column_data_t::initialize_scan(column_scan_state& state) {
        assert(state.child_states.size() == 2);

        state.row_index = 0;
        state.current = nullptr;

        validity.initialize_scan(state.child_states[0]);
        child_column->initialize_scan(state.child_states[1]);
    }

    void array_column_data_t::initialize_scan_with_offset(column_scan_state& state, uint64_t row_idx) {
        assert(state.child_states.size() == 2);

        if (row_idx == 0) {
            initialize_scan(state);
            return;
        }

        state.row_index = row_idx;
        state.current = nullptr;

        validity.initialize_scan_with_offset(state.child_states[0], row_idx);

        auto size = static_cast<types::array_logical_type_extention*>(type_.extention())->size();
        auto child_count = (row_idx - start_) * size;

        assert(child_count <= child_column->max_entry());
        if (child_count < child_column->max_entry()) {
            const auto child_offset = start_ + child_count;
            child_column->initialize_scan_with_offset(state.child_states[1], child_offset);
        }
    }

    uint64_t array_column_data_t::scan(uint64_t vector_index,
                                       column_scan_state& state,
                                       vector::vector_t& result,
                                       uint64_t count) {
        return scan_count(state, result, count);
    }

    uint64_t array_column_data_t::scan_committed(uint64_t vector_index,
                                                 column_scan_state& state,
                                                 vector::vector_t& result,
                                                 bool allow_updates,
                                                 uint64_t count) {
        return scan_count(state, result, count);
    }

    uint64_t array_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        auto scan_count = validity.scan_count(state.child_states[0], result, count);
        auto& child_vec = result.entry();
        auto size = static_cast<types::array_logical_type_extention*>(type_.extention())->size();
        child_column->scan_count(state.child_states[1], child_vec, count * size);
        return scan_count;
    }

    void array_column_data_t::skip(column_scan_state& state, uint64_t count) {
        validity.skip(state.child_states[0], count);
        auto size = static_cast<types::array_logical_type_extention*>(type_.extention())->size();
        child_column->skip(state.child_states[1], count * size);
    }

    void array_column_data_t::initialize_append(column_append_state& state) {
        column_append_state validity_append;
        validity.initialize_append(validity_append);
        state.child_appends.push_back(std::move(validity_append));

        column_append_state child_append;
        child_column->initialize_append(child_append);
        state.child_appends.push_back(std::move(child_append));
    }

    void array_column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
        if (vector.get_vector_type() != vector::vector_type::FLAT) {
            vector::vector_t append_vector(vector);
            append_vector.flatten(count);
            append(state, append_vector, count);
            return;
        }

        validity.append(state.child_appends[0], vector, count);
        auto& child_vec = vector.entry();
        auto size = static_cast<types::array_logical_type_extention*>(type_.extention())->size();
        child_column->append(state.child_appends[1], child_vec, count * size);

        count_ += count;
    }

    void array_column_data_t::revert_append(int64_t start_row) {
        validity.revert_append(start_row);
        auto size = static_cast<types::array_logical_type_extention*>(type_.extention())->size();
        child_column->revert_append(start_row * size);

        count_ = start_row - start_;
    }

    uint64_t array_column_data_t::fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) {
        throw std::logic_error("Function is not implemented: Array fetch");
    }

    void array_column_data_t::update(uint64_t column_index,
                                     vector::vector_t& update_vector,
                                     int64_t* row_ids,
                                     uint64_t update_count) {
        throw std::logic_error("Function is not implemented: Array update is not supported.");
    }

    void array_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                            vector::vector_t& update_vector,
                                            int64_t* row_ids,
                                            uint64_t update_count,
                                            uint64_t depth) {
        throw std::logic_error("Function is not implemented: Array update Column is not supported");
    }

    void array_column_data_t::fetch_row(column_fetch_state& state,
                                        int64_t row_id,
                                        vector::vector_t& result,
                                        uint64_t result_idx) {
        if (state.child_states.empty()) {
            state.child_states.push_back(std::make_unique<column_fetch_state>());
        }

        validity.fetch_row(*state.child_states[0], row_id, result, result_idx);

        auto& child_vec = result.entry();
        auto& child_type = type_.child_type();

        auto child_state = std::make_unique<column_scan_state>();
        child_state->initialize(child_type);
        auto size = static_cast<types::array_logical_type_extention*>(type_.extention())->size();
        const auto child_offset = start_ + (static_cast<uint64_t>(row_id) - start_) * size;

        child_column->initialize_scan_with_offset(*child_state, child_offset);
        vector::vector_t child_scan(resource_, child_type, size);
        child_column->scan_count(*child_state, child_scan, size);
        vector::vector_ops::copy(child_scan, child_vec, size, 0, result_idx * size);
    }

    void array_column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                      std::vector<uint64_t> col_path,
                                                      std::vector<column_segment_info>& result) {
        col_path.push_back(0);
        validity.get_column_segment_info(row_group_index, col_path, result);
        col_path.back() = 1;
        child_column->get_column_segment_info(row_group_index, col_path, result);
    }

} // namespace components::table