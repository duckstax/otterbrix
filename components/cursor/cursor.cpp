#include "cursor.hpp"

namespace components::cursor {

    error_t::error_t(error_code_t type)
        : type(type)
        , what() {}

    error_t::error_t(error_code_t type, const std::string& what)
        : type(type)
        , what(what) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource)
        : document_data_(resource)
        , table_data_(resource, {})
        , type_data_(resource)
        , error_(error_code_t::none)
        , success_(true) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource, const error_t& error)
        : document_data_(resource)
        , table_data_(resource, {})
        , type_data_(resource)
        , error_(error)
        , success_(false) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource, operation_status_t op_status)
        : document_data_(resource)
        , table_data_(resource, {})
        , type_data_(resource)
        , error_(error_code_t::none)
        , success_(op_status == operation_status_t::success) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<document::document_ptr>&& documents)
        : size_(documents.size())
        , document_data_(std::move(documents))
        , table_data_(resource, {})
        , type_data_(resource)
        , error_(error_code_t::none)
        , success_(true)
        , uses_table_data_(false) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : size_(chunk.size())
        , document_data_(resource)
        , table_data_(std::move(chunk))
        , type_data_(resource)
        , error_(error_code_t::none)
        , success_(true)
        , uses_table_data_(true) {}

    cursor_t::cursor_t(std::pmr::memory_resource* resource,
                       std::pmr::vector<components::types::complex_logical_type>&& types)
        : size_(types.size())
        , document_data_(resource)
        , table_data_(resource, {})
        , type_data_(std::move(types))
        , error_(error_code_t::none)
        , success_(true)
        , uses_table_data_(true) {}

    bool cursor_t::uses_table_data() const { return uses_table_data_; }
    std::pmr::vector<document::document_ptr>& cursor_t::document_data() { return document_data_; }
    const std::pmr::vector<document::document_ptr>& cursor_t::document_data() const { return document_data_; }
    vector::data_chunk_t& cursor_t::chunk_data() { return table_data_; }
    const vector::data_chunk_t& cursor_t::chunk_data() const { return table_data_; }
    std::pmr::vector<components::types::complex_logical_type>& cursor_t::type_data() { return type_data_; }
    const std::pmr::vector<components::types::complex_logical_type>& cursor_t::type_data() const { return type_data_; }

    std::size_t cursor_t::size() const { return size_; }
    bool cursor_t::has_next() const { return static_cast<std::size_t>(current_index_ + 1) < size_; }
    document::document_ptr cursor_t::next_document() {
        return get_document(static_cast<std::size_t>(++current_index_));
    }
    document::document_ptr cursor_t::get_document() const {
        return get_document(static_cast<std::size_t>(current_index_ < 0 ? 0 : current_index_));
    }
    document::document_ptr cursor_t::get_document(std::size_t index) const {
        if (index < size_) {
            return document_data_.at(index);
        }
        return nullptr;
    }

    bool cursor_t::is_success() const noexcept { return success_; }

    bool cursor_t::is_error() const noexcept { return !success_; }

    error_t cursor_t::get_error() const { return error_; }

    void cursor_t::sort(std::function<bool(document::document_ptr, document::document_ptr)> sorter) {
        std::sort(document_data_.begin(), document_data_.end(), std::move(sorter));
        current_index_ = start_index;
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, operation_status_t op_status) {
        return cursor_t_ptr{new cursor_t(resource, op_status)};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource) { return cursor_t_ptr{new cursor_t(resource)}; }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, error_code_t type, const std::string& what) {
        return cursor_t_ptr{new cursor_t(resource, error_t(type, what))};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<document::document_ptr>&& documents) {
        return cursor_t_ptr{new cursor_t(resource, std::move(documents))};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        return cursor_t_ptr{new cursor_t(resource, std::move(chunk))};
    }

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::types::complex_logical_type>&& types) {
        return cursor_t_ptr{new cursor_t(resource, std::move(types))};
    }
} // namespace components::cursor
