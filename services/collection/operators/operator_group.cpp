#include "operator_group.hpp"
#include <services/collection/collection.hpp>
#include <services/collection/operators/operator_empty.hpp>

namespace services::collection::operators {

    operator_group_t::operator_group_t(context_collection_t* context)
        : read_write_operator_t(context, operator_type::aggregate)
        , keys_(context->resource())
        , values_(context->resource())
        , input_documents_(context->resource()) {}

    void operator_group_t::add_key(const std::string &name, get::operator_get_ptr &&getter) {
        keys_.push_back({name, std::move(getter)});
    }

    void operator_group_t::add_value(const std::string &name, aggregate::operator_aggregate_ptr &&aggregator) {
        values_.push_back({name, std::move(aggregator)});
    }

    void operator_group_t::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            output_ = make_operator_data(context_->resource());
            create_list_documents();
            calc_aggregate_values(pipeline_context);
        }
    }

    void operator_group_t::create_list_documents() {
        for (const auto& doc : left_->output()->documents()) {
            auto new_doc = components::document::make_document();
            bool is_valid = true;
            for (const auto& key : keys_) {
                auto value = key.getter->value(doc);
                if (value) {
                    new_doc->set(key.name, *value);
                } else {
                    is_valid = false;
                    break;
                }
            }
            if (is_valid) {
                bool is_new = true;
                for (std::size_t i = 0; i < output_->documents().size(); ++i) {
                    if (is_equals_documents(new_doc, output_->documents().at(i))) {
                        input_documents_.at(i)->append(doc);
                        is_new = false;
                        break;
                    }
                }
                if (is_new) {
                    output_->append(new_doc);
                    auto input_doc = make_operator_data(context_->resource());
                    input_doc->append(doc);
                    input_documents_.push_back(std::move(input_doc));
                }
            }
        }
    }

    void operator_group_t::calc_aggregate_values(components::pipeline::context_t* pipeline_context) {
        for (const auto &value : values_) {
            auto &aggregator = value.aggregator;
            for (std::size_t i = 0; i < output_->documents().size(); ++i) {
                auto &document = output_->documents().at(i);
                aggregator->clear(); //todo: need copy aggregator
                aggregator->set_children(std::make_unique<operator_empty_t>(context_, input_documents_.at(i)->copy()));
                aggregator->on_execute(pipeline_context);
                document->set(value.name, *aggregator->value());
            }
        }
    }

} // namespace services::collection::operators
