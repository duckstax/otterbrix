#include "operator_aggregate.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(context_collection_t* context)
        : read_only_operator_t(context, operator_type::aggregate) {}

    void operator_aggregate_t::on_execute_impl(components::pipeline::context_t*) {
        auto resource = left_ && left_->output() ? left_->output()->resource() : context_->resource();
        output_ = make_operator_data(resource);
        output_->append(aggregate_impl());
    }

    void operator_aggregate_t::set_value(document_ptr& doc, std::string_view key) const {
        doc->set(key, output_->documents().at(0), key_impl());
    }

    components::document::value_t operator_aggregate_t::value() const {
        return output_->documents().at(0)->get_value(key_impl());
    }
} // namespace services::collection::operators::aggregate
