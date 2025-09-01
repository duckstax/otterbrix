#include "operator_data.hpp"

namespace components::base::operators {

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , data_(std::pmr::vector<document::document_ptr>(resource)) {}

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<types::complex_logical_type>& types,
                                     uint64_t capacity)
        : resource_(resource)
        , data_(vector::data_chunk_t(resource, types, capacity)) {}

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : resource_(resource)
        , data_(std::move(chunk)) {}

    operator_data_t::ptr operator_data_t::copy() const {
        if (std::holds_alternative<std::pmr::vector<document::document_ptr>>(data_)) {
            auto& documents = std::get<std::pmr::vector<document::document_ptr>>(data_);
            auto copy_data = make_operator_data(resource_);
            std::get<std::pmr::vector<document::document_ptr>>(copy_data->data_).reserve(documents.size());
            for (const auto& document : documents) {
                copy_data->append(document);
            }
            return copy_data;
        } else {
            auto& data = std::get<vector::data_chunk_t>(data_);
            auto copy_data = make_operator_data(resource_, data.types(), data.size());
            data.copy(std::get<vector::data_chunk_t>(copy_data->data_), 0);
            return copy_data;
        }
    }

    std::size_t operator_data_t::size() const {
        if (std::holds_alternative<std::pmr::vector<document::document_ptr>>(data_)) {
            return std::get<std::pmr::vector<document::document_ptr>>(data_).size();
        } else {
            return std::get<vector::data_chunk_t>(data_).size();
        }
    }

    std::pmr::vector<document::document_ptr>& operator_data_t::documents() {
        return std::get<std::pmr::vector<document::document_ptr>>(data_);
    }

    const std::pmr::vector<document::document_ptr>& operator_data_t::documents() const {
        return std::get<std::pmr::vector<document::document_ptr>>(data_);
    }

    vector::data_chunk_t& operator_data_t::data_chunk() { return std::get<vector::data_chunk_t>(data_); }

    const vector::data_chunk_t& operator_data_t::data_chunk() const { return std::get<vector::data_chunk_t>(data_); }

    bool operator_data_t::uses_data_chunk() const { return std::holds_alternative<vector::data_chunk_t>(data_); }

    bool operator_data_t::uses_documents() const {
        return std::holds_alternative<std::pmr::vector<document::document_ptr>>(data_);
    }

    std::pmr::memory_resource* operator_data_t::resource() { return resource_; }

    void operator_data_t::append(document::document_ptr document) {
        std::get<std::pmr::vector<document::document_ptr>>(data_).push_back(std::move(document));
    }

    void operator_data_t::append(vector::vector_t row) {
        auto& chunk = std::get<vector::data_chunk_t>(data_);
        size_t index = chunk.size();
        for (size_t i = 0; i < row.size(); i++) {
            chunk.set_value(0, index, row.value(i));
        }
    }

} // namespace components::base::operators
