#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators::merge {

    class operator_merge_t : public read_only_operator_t {
    public:
        explicit operator_merge_t(context_collection_t* context, components::logical_plan::limit_t limit);

    protected:
        components::logical_plan::limit_t limit_;

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
        virtual void on_merge_impl(components::pipeline::context_t* pipeline_context) = 0;
    };

    using operator_merge_ptr = boost::intrusive_ptr<operator_merge_t>;

    bool is_operator_merge(const components::expressions::compare_expression_ptr& expr);
    operator_merge_ptr create_operator_merge(context_collection_t* context,
                                             const components::expressions::compare_type& type,
                                             components::logical_plan::limit_t limit);
    operator_merge_ptr create_operator_merge(context_collection_t* context,
                                             const components::expressions::compare_expression_ptr& expr,
                                             components::logical_plan::limit_t limit);

} // namespace services::collection::operators::merge
