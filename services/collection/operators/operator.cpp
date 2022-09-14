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
        , type_(type) {
    }

    void operator_t::on_execute(operator_data_t* data) {
        if (state_ == operator_state::created || state_ == operator_state::executed) { //todo: delete second operand
            state_ = operator_state::running;
            on_execute_impl(data);
            state_ = operator_state::executed;
        }
    }

    operator_state operator_t::state() const {
        return state_;
    }


    read_only_operator_t::read_only_operator_t(context_collection_t* collection, operator_type type)
        : operator_t(collection, type) {
    }


    read_write_operator_t::read_write_operator_t(context_collection_t* collection, operator_type type)
        : operator_t(collection, type)
        , state_(read_write_operator_state::pending) {
    }

} // namespace services::collection::operators
