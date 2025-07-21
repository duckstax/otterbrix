#include "operator_merge.hpp"
#include "operator_and.hpp"
#include "operator_not.hpp"
#include "operator_or.hpp"

namespace components::collection::operators::merge {

    operator_merge_t::operator_merge_t(services::collection::context_collection_t* context, logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , limit_(limit) {}

    void operator_merge_t::on_execute_impl(pipeline::context_t* pipeline_context) { on_merge_impl(pipeline_context); }

    bool is_operator_merge(const expressions::compare_expression_ptr& expr) {
        return expr->is_union() && !expr->children().empty();
    }

    operator_merge_ptr create_operator_merge(services::collection::context_collection_t* context,
                                             const expressions::compare_type& type,
                                             logical_plan::limit_t limit) {
        using expressions::compare_type;
        switch (type) {
            case compare_type::union_and:
                return {new operator_and_t(context, limit)};
            case compare_type::union_or:
                return {new operator_or_t(context, limit)};
            case compare_type::union_not:
                return {new operator_not_t(context, limit)};
            default:
                break;
        }
        return nullptr;
    }

    operator_merge_ptr create_operator_merge(services::collection::context_collection_t* context,
                                             const expressions::compare_expression_ptr& expr,
                                             logical_plan::limit_t limit) {
        return create_operator_merge(context, expr->type(), limit);
    }

} // namespace components::collection::operators::merge