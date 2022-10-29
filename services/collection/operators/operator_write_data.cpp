#include "operator_write_data.hpp"

namespace services::collection::operators {

    operator_write_data_t::operator_write_data_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , documents_(resource) {
    }

    operator_write_data_t::ptr operator_write_data_t::copy() const {
        auto copy_data = make_operator_write_data(resource_);
        copy_data->documents_.reserve(documents_.size());
        for (const auto &document : documents_) {
            copy_data->documents_.push_back(document);
        }
        return copy_data;
    }

    std::size_t operator_write_data_t::size() const {
        return documents_.size();
    }

    std::pmr::vector<operator_write_data_t::document_id_t>& operator_write_data_t::documents() {
        return documents_;
    }

    void operator_write_data_t::append(document_id_t document) {
        documents_.push_back(std::move(document));
    }

}
