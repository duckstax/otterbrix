#include "operator_merge.hpp"
#include "operator_and.hpp"
#include "operator_not.hpp"
#include "operator_or.hpp"

namespace services::collection::operators::merge {

    operator_merge_t::operator_merge_t(context_collection_t* context, components::ql::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , limit_(limit) {}

    void operator_merge_t::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        on_merge_impl(pipeline_context);
    }

    bool is_operator_merge(const components::expressions::compare_expression_ptr& expr) {
        return expr->is_union() && !expr->children().empty();
    }

    operator_merge_ptr create_operator_merge(context_collection_t* context,
                                             const components::expressions::compare_type& type,
                                             components::ql::limit_t limit) {
        using components::expressions::compare_type;
        switch (type) {
            case compare_type::union_and:
                return std::make_unique<operator_and_t>(context, limit);
            case compare_type::union_or:
                return std::make_unique<operator_or_t>(context, limit);
            case compare_type::union_not:
                return std::make_unique<operator_not_t>(context, limit);
            default:
                break;
        }
        return nullptr;
    }

    operator_merge_ptr create_operator_merge(context_collection_t* context,
                                             const components::expressions::compare_expression_ptr& expr,
                                             components::ql::limit_t limit) {
        return create_operator_merge(context, expr->type(), limit);
    }

} // namespace services::collection::operators::merge