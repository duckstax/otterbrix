#include "operator_group.hpp"

#include "transformation.hpp"

#include <components/physical_plan/base/operators/operator_empty.hpp>
#include <services/collection/collection.hpp>

namespace components::table::operators {

    operator_group_t::operator_group_t(services::collection::context_collection_t* context)
        : read_write_operator_t(context, operator_type::aggregate)
        , keys_(context_->resource())
        , values_(context_->resource())
        , inputs_(context_->resource())
        , transposed_output_(context_->resource()) {}

    operator_group_t::operator_group_t(std::pmr::memory_resource* resource)
        : read_write_operator_t(nullptr, operator_type::aggregate)
        , keys_(resource)
        , values_(resource)
        , inputs_(context_->resource())
        , transposed_output_(context_->resource()) {}

    void operator_group_t::add_key(const std::string& name, get::operator_get_ptr&& getter) {
        keys_.push_back({name, std::move(getter)});
    }

    void operator_group_t::add_value(const std::string& name, aggregate::operator_aggregate_ptr&& aggregator) {
        values_.push_back({name, std::move(aggregator)});
    }

    void operator_group_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            output_ =
                base::operators::make_operator_data(left_->output()->resource(), left_->output()->data_chunk().types());
            create_list_documents();
            calc_aggregate_values(pipeline_context);
            output_ = base::operators::make_operator_data(left_->output()->resource(),
                                                          impl::transpose(left_->output()->resource(),
                                                                          transposed_output_,
                                                                          left_->output()->data_chunk().types()));
        }
    }

    void operator_group_t::create_list_documents() {
        auto& chunk = left_->output()->data_chunk();

        auto matrix = impl::transpose(left_->output()->resource(), chunk);

        for (const auto& row : matrix) {
            std::pmr::vector<types::logical_value_t> new_row(row.get_allocator().resource());
            bool is_valid = true;

            for (const auto& key : keys_) {
                auto value = key.getter->value(row);
                if (value.is_null()) {
                    is_valid = false;
                    break;
                } else {
                    value.set_alias(key.name);
                    new_row.emplace_back(std::move(value));
                }
            }
            if (is_valid) {
                bool is_new = true;
                for (size_t i = 0; i < transposed_output_.size(); i++) {
                    if (new_row == transposed_output_[i]) {
                        inputs_.at(i).emplace_back(row);
                        is_new = false;
                        break;
                    }
                }
                if (is_new) {
                    transposed_output_.emplace_back(std::move(new_row));
                    auto input = impl::value_matrix_t(row.get_allocator().resource());
                    input.emplace_back(row);
                    inputs_.emplace_back(std::move(input));
                }
            }
        }
    }

    void operator_group_t::calc_aggregate_values(pipeline::context_t* pipeline_context) {
        auto types = left_->output()->data_chunk().types();
        for (const auto& value : values_) {
            auto& aggregator = value.aggregator;
            for (size_t i = 0; i < transposed_output_.size(); i++) {
                aggregator->clear(); //todo: need copy aggregator
                aggregator->set_children(boost::intrusive_ptr(new components::base::operators::operator_empty_t(
                    context_,
                    base::operators::make_operator_data(context_->resource(),
                                                        impl::transpose(context_->resource(), inputs_.at(i), types)))));
                aggregator->on_execute(pipeline_context);
                aggregator->set_value(transposed_output_.at(i), value.name);
            }
        }
    }

} // namespace components::table::operators
