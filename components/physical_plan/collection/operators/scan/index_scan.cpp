#include "index_scan.hpp"
#include <components/index/disk/route.hpp>
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    using range = components::index::index_t::range;

    std::vector<range> search_range_by_index(components::index::index_t* index,
                                             const components::expressions::compare_expression_ptr& expr,
                                             const components::logical_plan::storage_parameters* parameters) {
        using components::expressions::compare_type;
        using components::logical_plan::get_parameter;
        auto value = get_parameter(parameters, expr->value());
        switch (expr->type()) {
            case compare_type::eq:
                return {index->find(value)};
            case compare_type::ne:
                return {index->lower_bound(value), index->upper_bound(value)};
            case compare_type::gt:
                return {index->upper_bound(value)};
            case compare_type::lt:
                return {index->lower_bound(value)};
            case compare_type::gte:
                return {index->find(value), index->upper_bound(value)};
            case compare_type::lte:
                return {index->lower_bound(value), index->find(value)};
            default:
                //todo: error
                return {{index->cend(), index->cend()}, {index->cend(), index->cend()}};
        }
    }

    void search_by_index(components::index::index_t* index,
                         const components::expressions::compare_expression_ptr& expr,
                         const components::logical_plan::limit_t& limit,
                         const components::logical_plan::storage_parameters* parameters,
                         operator_data_ptr& result) {
        auto ranges = search_range_by_index(index, expr, parameters);
        int count = 0;
        for (const auto& range : ranges) {
            for (auto it = range.first; it != range.second; ++it) {
                if (!limit.check(count)) {
                    return;
                }
                result->append(it->doc); //todo: check nullptr and get by id
                ++count;
            }
        }
    }

    index_scan::index_scan(context_collection_t* context,
                           components::expressions::compare_expression_ptr expr,
                           components::logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , expr_(std::move(expr))
        , limit_(limit) {}

    void index_scan::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        trace(context_->log(), "index_scan by field \"{}\"", expr_->key_left().as_string());
        auto* index = components::index::search_index(context_->index_engine(), {expr_->key_left()});
        if (index && index->is_disk()) {
            trace(context_->log(), "index_scan: send query into disk");
            auto value = components::logical_plan::get_parameter(&pipeline_context->parameters, expr_->value());
            pipeline_context->send(index->disk_agent(), index::handler_id(index::route::find), value, expr_->type());
            async_wait();
        } else {
            trace(context_->log(), "index_scan: prepare result");
            if (!limit_.check(0)) {
                return; //limit = 0
            }
            output_ = make_operator_data(context_->resource());
            if (index) {
                search_by_index(index, expr_, limit_, &pipeline_context->parameters, output_);
            }
        }
    }

    void index_scan::on_resume_impl(components::pipeline::context_t* pipeline_context) {
        trace(context_->log(), "resume index_scan by field \"{}\"", expr_->key_left().as_string());
        auto* index = components::index::search_index(context_->index_engine(), {expr_->key_left()});
        trace(context_->log(), "index_scan: prepare result");
        if (!limit_.check(0)) {
            return; //limit = 0
        }
        output_ = make_operator_data(context_->resource());
        if (index) {
            search_by_index(index, expr_, limit_, &pipeline_context->parameters, output_);
        }
    }

} // namespace services::collection::operators
