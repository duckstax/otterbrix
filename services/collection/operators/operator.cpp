#include "operator.hpp"

namespace services::collection::operators {

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource)
        : documents_(resource) {
    }

    std::size_t operator_data_t::size() const {
        return documents_.size();
    }

    std::pmr::vector<operator_data_t::document_ptr>& operator_data_t::documents() {
        return documents_;
    }

    void operator_data_t::append(document_ptr document) {
        documents_.push_back(std::move(document));
    }


    operator_t::operator_t(context_collection_t* context, operator_type type)
        : context_(context)
        , operator_type_(type) {
    }

    void operator_t::on_execute(operator_data_t* data) {
        return on_execute_impl(data);
    }

} // namespace services::collection::operators
