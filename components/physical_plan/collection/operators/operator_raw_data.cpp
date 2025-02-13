#include "operator_raw_data.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    operator_raw_data_t::operator_raw_data_t(std::pmr::vector<document_ptr>&& documents)
        : read_only_operator_t(nullptr, operator_type::raw_data) {
        output_ = make_operator_data(documents.get_allocator().resource());
        output_->documents() = std::move(documents);
    }

    operator_raw_data_t::operator_raw_data_t(const std::pmr::vector<document_ptr>& documents)
        : read_only_operator_t(nullptr, operator_type::raw_data) {
        output_ = make_operator_data(documents.get_allocator().resource());
        output_->documents() = documents;
    }

    void operator_raw_data_t::on_execute_impl(components::pipeline::context_t*) {}

} // namespace services::collection::operators
