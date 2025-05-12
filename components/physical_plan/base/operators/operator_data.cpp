#include "operator_data.hpp"

namespace services::base::operators {

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , data_(std::pmr::vector<components::document::document_ptr>(resource)) {}

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource,
                                     const std::vector<components::types::complex_logical_type>& types,
                                     uint64_t capacity)
        : resource_(resource)
        , data_(components::vector::data_chunk_t(resource, types, capacity)) {}

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk)
        : resource_(resource)
        , data_(std::move(chunk)) {}

    operator_data_t::ptr operator_data_t::copy() const {
        if (std::holds_alternative<std::pmr::vector<components::document::document_ptr>>(data_)) {
            auto& documents = std::get<std::pmr::vector<components::document::document_ptr>>(data_);
            auto copy_data = make_operator_data(resource_);
            std::get<std::pmr::vector<components::document::document_ptr>>(copy_data->data_).reserve(documents.size());
            for (const auto& document : documents) {
                copy_data->append(document);
            }
            return copy_data;
        } else {
            auto& data = std::get<components::vector::data_chunk_t>(data_);
            auto copy_data = make_operator_data(resource_, data.types(), data.size());
            data.copy(std::get<components::vector::data_chunk_t>(copy_data->data_), 0);
            return copy_data;
        }
    }

    std::size_t operator_data_t::size() const {
        if (std::holds_alternative<std::pmr::vector<components::document::document_ptr>>(data_)) {
            return std::get<std::pmr::vector<components::document::document_ptr>>(data_).size();
        } else {
            return std::get<components::vector::data_chunk_t>(data_).size();
        }
    }

    data_t& operator_data_t::data() { return data_; }

    std::pmr::memory_resource* operator_data_t::resource() { return resource_; }

    void operator_data_t::append(components::document::document_ptr document) {
        std::get<std::pmr::vector<components::document::document_ptr>>(data_).push_back(std::move(document));
    }

    void operator_data_t::append(components::vector::vector_t row) {
        auto& chunk = std::get<components::vector::data_chunk_t>(data_);
        size_t index = chunk.size();
        for (size_t i = 0; i < row.size(); i++) {
            chunk.set_value(0, index, row.value(i));
        }
    }

} // namespace services::base::operators
