#include "index_scan.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    using range = components::index::index_t::range;

    std::vector<range> search_range_by_index(components::index::index_t* index, const components::ql::expr_ptr &expr) {
        using components::ql::condition_type;
        switch (expr->type_) {
            case condition_type::eq:
                return {index->find(expr->value_)};
            case condition_type::ne:
                return {index->lower_bound(expr->value_), index->upper_bound(expr->value_)};
            case condition_type::gt:
                return {index->upper_bound(expr->value_)};
            case condition_type::lt:
                return {index->lower_bound(expr->value_)};
            case condition_type::gte:
                return {index->find(expr->value_), index->upper_bound(expr->value_)};
            case condition_type::lte:
                return {index->lower_bound(expr->value_), index->find(expr->value_)};
            default:
                //todo: error
                return {{index->cend(), index->cend()}, {index->cend(), index->cend()}};
        }
    }

    void search_by_index(components::index::index_t* index, const components::ql::expr_ptr &expr, const predicates::limit_t &limit, operator_data_ptr &result) {
        auto ranges = search_range_by_index(index, expr);
        int count = 0;
        for (const auto &range : ranges) {
            for (auto it = range.first; it != range.second; ++it) {
                if (!limit.check(count)) {
                    return;
                }
                result->append(*it);
                ++count;
            }
        }
    }


    index_scan::index_scan(context_collection_t* context, components::ql::expr_ptr expr, predicates::limit_t limit)
        : read_only_operator_t(context, operator_type::full_scan)
        , expr_(std::move(expr))
        , limit_(limit) {
    }

    void index_scan::on_execute_impl(planner::transaction_context_t* transaction_context) {
        if (!limit_.check(0)) {
            return; //limit = 0
        }
        output_ = make_operator_data(context_->resource());
        auto* index = components::index::search_index(context_->index_engine(), {expr_->key_});
        if (index) {
            search_by_index(index, expr_, limit_, output_);
        }
    }

} // namespace services::collection::operators
