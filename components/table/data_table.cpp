#include "data_table.hpp"

#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <unordered_set>

#include "row_group.hpp"

namespace components::table {

    data_table_t::data_table_t(std::pmr::memory_resource* resource,
                               storage::block_manager_t& block_manager,
                               std::vector<column_definition_t> column_definitions,
                               std::string name)
        : resource_(resource)
        , column_definitions_(std::move(column_definitions))
        , is_root_(true)
        , name_(std::move(name)) {
        this->row_groups_ = std::make_shared<collection_t>(resource_, block_manager, types(), 0);
    }

    data_table_t::data_table_t(data_table_t& parent, column_definition_t& new_column)
        : resource_(parent.resource_)
        , is_root_(true) {
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def.copy());
        }
        column_definitions_.emplace_back(new_column.copy());

        std::lock_guard parent_lock(parent.append_lock_);

        this->row_groups_ = parent.row_groups_->add_column(new_column);

        parent.is_root_ = false;
    }

    data_table_t::data_table_t(data_table_t& parent, uint64_t removed_column)
        : resource_(parent.resource_)
        , is_root_(true) {
        std::lock_guard parent_lock(parent.append_lock_);

        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def.copy());
        }

        assert(removed_column < column_definitions_.size());
        column_definitions_.erase(column_definitions_.begin() + removed_column);

        uint64_t storage_idx = 0;
        for (uint64_t i = 0; i < column_definitions_.size(); i++) {
            auto& col = column_definitions_[i];
            col.set_oid(i);
            col.set_storage_oid(storage_idx++);
        }

        this->row_groups_ = parent.row_groups_->remove_column(removed_column);

        parent.is_root_ = false;
    }

    data_table_t::data_table_t(data_table_t& parent,
                               uint64_t changed_idx,
                               const types::complex_logical_type& target_type,
                               const std::vector<storage_index_t>& bound_columns)
        : resource_(parent.resource_)
        , is_root_(true) {
        std::lock_guard lock(append_lock_);
        for (auto& column_def : parent.column_definitions_) {
            column_definitions_.emplace_back(column_def.copy());
        }

        column_definitions_[changed_idx].type() = target_type;

        // TODO: type casting
        // this->row_groups_ = parent.row_groups_->alter_type(resource_, changed_idx, target_type, bound_columns);

        // replaces the previous; the parent is no longer the root data_table
        parent.is_root_ = false;
    }

    std::vector<types::complex_logical_type> data_table_t::types() {
        std::vector<types::complex_logical_type> types;
        for (auto& it : column_definitions_) {
            types.push_back(it.type());
        }
        return types;
    }

    const std::vector<column_definition_t>& data_table_t::columns() const { return column_definitions_; }

    void data_table_t::initialize_scan(table_scan_state& state,
                                       const std::vector<storage_index_t>& column_ids,
                                       table_filter_set_t* table_filters) {
        state.initialize(column_ids, table_filters);
        row_groups_->initialize_scan(state.table_state, column_ids, table_filters);
    }

    void data_table_t::initialize_scan_with_offset(table_scan_state& state,
                                                   const std::vector<storage_index_t>& column_ids,
                                                   uint64_t start_row,
                                                   uint64_t end_row) {
        state.initialize(column_ids);
        row_groups_->initialize_scan_with_offset(state.table_state, column_ids, start_row, end_row);
    }

    uint64_t data_table_t::row_group_size() const { return row_groups_->row_group_size(); }

    void data_table_t::scan(vector::data_chunk_t& result, table_scan_state& state) { state.table_state.scan(result); }

    bool data_table_t::create_index_scan(table_scan_state& state, vector::data_chunk_t& result, table_scan_type type) {
        return state.table_state.scan_committed(result, type);
    }

    std::string data_table_t::table_name() const { return name_; }

    void data_table_t::set_table_name(std::string new_name) { name_ = std::move(new_name); }

    void data_table_t::fetch(vector::data_chunk_t& result,
                             const std::vector<storage_index_t>& column_ids,
                             const vector::vector_t& row_identifiers,
                             uint64_t fetch_count,
                             column_fetch_state& state) {
        row_groups_->fetch(result, column_ids, row_identifiers, fetch_count, state);
    }

    std::unique_ptr<constraint_state> data_table_t::initialize_constraint_state(
        const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        return std::make_unique<constraint_state>(bound_constraints);
    }

    void data_table_t::append_lock(table_append_state& state) {
        state.append_lock = std::unique_lock(append_lock_);
        if (!is_root_) {
            throw std::logic_error("Transaction conflict: adding entries to a table that has been altered!");
        }
        state.row_start = static_cast<int64_t>(row_groups_->total_rows());
        state.current_row = state.row_start;
    }

    void data_table_t::initialize_append(table_append_state& state) {
        if (!state.append_lock) {
            throw std::logic_error("data_table_t::append_lock should be called before data_table_t::initialize_append");
        }
        row_groups_->initialize_append(state);
    }

    void data_table_t::append(vector::data_chunk_t& chunk, table_append_state& state) {
        assert(is_root_);
        row_groups_->append(chunk, state);
    }

    void data_table_t::finalize_append(table_append_state& state) { row_groups_->finalize_append(state); }

    void data_table_t::scan_table_segment(uint64_t row_start,
                                          uint64_t count,
                                          const std::function<void(vector::data_chunk_t& chunk)>& function) {
        if (count == 0) {
            return;
        }
        uint64_t end = row_start + count;

        std::vector<storage_index_t> column_ids;
        std::vector<types::complex_logical_type> types;
        for (uint64_t i = 0; i < this->column_definitions_.size(); i++) {
            auto& col = this->column_definitions_[i];
            column_ids.emplace_back(i);
            types.push_back(col.type());
        }
        vector::data_chunk_t chunk(resource_, types);

        create_index_scan_state state(resource_);

        initialize_scan_with_offset(state, column_ids, row_start, row_start + count);
        auto row_start_aligned =
            state.table_state.row_group->start + state.table_state.vector_index * vector::DEFAULT_VECTOR_CAPACITY;

        uint64_t current_row = row_start_aligned;
        while (current_row < end) {
            state.table_state.scan_committed(chunk, table_scan_type::COMMITTED_ROWS);
            if (chunk.size() == 0) {
                break;
            }
            uint64_t end_row = current_row + chunk.size();
            uint64_t chunk_start = std::max<uint64_t>(current_row, row_start);
            uint64_t chunk_end = std::min<uint64_t>(end_row, end);
            assert(chunk_start < chunk_end);
            uint64_t chunk_count = chunk_end - chunk_start;
            if (chunk_count != chunk.size()) {
                assert(chunk_count <= chunk.size());
                uint64_t start_in_chunk;
                if (current_row >= row_start) {
                    start_in_chunk = 0;
                } else {
                    start_in_chunk = row_start - current_row;
                }
                vector::indexing_vector_t indexing(resource_, start_in_chunk, chunk_count);
                chunk.slice(indexing, chunk_count);
            }
            function(chunk);
            chunk.reset();
            current_row = end_row;
        }
    }

    void data_table_t::merge_storage(collection_t& data) { row_groups_->merge_storage(data); }

    std::unique_ptr<table_delete_state>
    data_table_t::initialize_delete(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        std::vector<types::complex_logical_type> types;
        auto result = std::make_unique<table_delete_state>(resource_, types);
        if (result->has_delete_constraints) {
            for (uint64_t i = 0; i < column_definitions_.size(); i++) {
                result->col_ids.emplace_back(column_definitions_[i].storage_oid());
                types.emplace_back(column_definitions_[i].type());
            }
            result->constraint = std::make_unique<constraint_state>(bound_constraints);
        }
        return result;
    }

    uint64_t data_table_t::delete_rows(table_delete_state& state, vector::vector_t& row_identifiers, uint64_t count) {
        assert(row_identifiers.type().type() == types::logical_type::BIGINT);
        if (count == 0) {
            return 0;
        }

        row_identifiers.flatten(count);
        auto ids = row_identifiers.data<int64_t>();

        uint64_t pos = 0;
        uint64_t delete_count = 0;
        while (pos < count) {
            uint64_t start = pos;
            bool is_transaction_delete = ids[pos] >= MAX_ROW_ID;
            for (pos++; pos < count; pos++) {
                bool row_is_transaction_delete = ids[pos] >= MAX_ROW_ID;
                if (row_is_transaction_delete != is_transaction_delete) {
                    break;
                }
            }
            uint64_t current_offset = start;
            uint64_t current_count = pos - start;

            vector::vector_t offset_ids(row_identifiers, current_offset, pos);
            delete_count += row_groups_->delete_rows(*this, ids + current_offset, current_count);
        }
        return delete_count;
    }

    std::unique_ptr<table_update_state>
    data_table_t::initialize_update(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints) {
        auto result = std::make_unique<table_update_state>();
        result->constraint = initialize_constraint_state(bound_constraints);
        return result;
    }

    /*
	void data_table_t::update(table_update_state &state, vector::vector_t &row_ids,
	                       const std::vector<uint64_t> &column_ids, vector::data_chunk_t &updates) {
		assert(row_ids.type().type() == types::logical_type::BIGINT);
		assert(column_ids.size() == updates.column_count());

		auto count = updates.size();
		if (count == 0) {
			return;
		}

		if (!is_root_) {
			throw std::logic_error("Transaction conflict: cannot update a table that has been altered!");
		}

		// now perform the actual update
		vector::vector_t max_row_id_vec(resource_, types::logical_value_t(static_cast<int64_t>(MAX_ROW_ID)));
		vector::vector_t row_ids_slice(resource_, types::logical_type::BIGINT);
		vector::data_chunk_t updates_slice;
		updates_slice.initialize_empty(resource_, updates.types());

		vector::indexing_vector_t indexing_local_update(resource_, count), indexing_global_update(resource_, count);
		auto n_local_update = vector::vector_ops::GreaterThanEquals(row_ids, max_row_id_vec, nullptr, count,
		                                                                  &indexing_local_update, &indexing_global_update);
		auto n_global_update = count - n_local_update;
		// otherwise global storage
		if (n_global_update > 0) {
			updates_slice.slice(updates, indexing_global_update, n_global_update);
			updates_slice.flatten();
			row_ids_slice.slice(row_ids, indexing_global_update, n_global_update);
			row_ids_slice.flatten(n_global_update);
			row_groups_->update(row_ids_slice.data<int64_t>(), column_ids, updates_slice);
		}
	}
	*/

    void data_table_t::update_column(vector::vector_t& row_ids,
                                     const std::vector<uint64_t>& column_path,
                                     vector::data_chunk_t& updates) {
        assert(row_ids.type().type() == types::logical_type::BIGINT);
        assert(updates.column_count() == 1);
        if (updates.size() == 0) {
            return;
        }

        if (!is_root_) {
            throw std::logic_error("Transaction conflict: cannot update a table that has been altered!");
        }

        updates.flatten();
        row_ids.flatten(updates.size());
        row_groups_->update_column(row_ids, column_path, updates);
    }

    uint64_t data_table_t::column_count() const { return column_definitions_.size(); }

    uint64_t data_table_t::total_rows() const { return row_groups_->total_rows(); }

    std::vector<column_segment_info> data_table_t::get_column_segment_info() {
        return row_groups_->get_column_segment_info();
    }

} // namespace components::table