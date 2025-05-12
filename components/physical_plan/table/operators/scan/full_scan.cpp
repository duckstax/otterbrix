#include "full_scan.hpp"

#include <components/physical_plan/table/operators/transformation.hpp>
#include <services/collection/collection.hpp>

namespace services::table::operators {

    std::unique_ptr<components::table::table_filter_t>
    transform_predicate(const components::expressions::compare_expression_ptr& exresssion,
                        const std::vector<components::types::complex_logical_type> types,
                        const components::logical_plan::storage_parameters* parameters) {
        switch (exresssion->type()) {
            case components::expressions::compare_type::union_and: {
                auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
                filter->child_filters.reserve(exresssion->children().size());
                for (const auto& child : exresssion->children()) {
                    filter->child_filters.emplace_back(transform_predicate(child, types, parameters));
                }
                return filter;
            }
            case components::expressions::compare_type::union_or: {
                auto filter = std::make_unique<components::table::conjunction_or_filter_t>();
                filter->child_filters.reserve(exresssion->children().size());
                for (const auto& child : exresssion->children()) {
                    filter->child_filters.emplace_back(transform_predicate(child, types, parameters));
                }
                return filter;
            }
            case components::expressions::compare_type::invalid:
                throw std::runtime_error("unsupported compare_type in expression to filter conversion");
            default: {
                auto it =
                    std::find_if(types.begin(), types.end(), [&](const components::types::complex_logical_type& type) {
                        return type.alias() == exresssion->key_left().as_string();
                    });
                assert(it != types.end());
                return std::make_unique<components::table::constant_filter_t>(
                    exresssion->type(),
                    impl::convert(parameters->parameters.at(exresssion->value())),
                    it - types.begin());
            }
        }
    }

    full_scan::full_scan(collection::context_collection_t* context,
                         const components::expressions::compare_expression_ptr& exresssion,
                         components::logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , exresssion_(exresssion)
        , limit_(limit) {}

    void full_scan::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        trace(context_->log(), "full_scan");
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }

        auto types = context_->table_storage().table().copy_types();
        output_ = base::operators::make_operator_data(context_->resource(), types);
        std::vector<components::table::storage_index_t> column_indices;
        column_indices.reserve(context_->table_storage().table().column_count());
        for (int64_t i = 0; i < context_->table_storage().table().column_count(); i++) {
            column_indices.emplace_back(i);
        }
        components::table::table_scan_state state(std::pmr::get_default_resource());
        auto filter =
            transform_predicate(exresssion_, types, pipeline_context ? &pipeline_context->parameters : nullptr);
        context_->table_storage().table().initialize_scan(state, column_indices, filter.get());
        // TODO: check limit inside scan
        context_->table_storage().table().scan(std::get<components::vector::data_chunk_t>(output_->data()), state);
    }

} // namespace services::table::operators
