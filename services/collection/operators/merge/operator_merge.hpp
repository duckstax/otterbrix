#pragma once

#include <components/expressions/compare_expression.hpp>
#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/predicates/limit.hpp>

namespace services::collection::operators::merge {

    class operator_merge_t : public read_only_operator_t {
    public:
        explicit operator_merge_t(context_collection_t* context, predicates::limit_t limit);

    protected:
        predicates::limit_t limit_;

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
        virtual void on_merge_impl(components::pipeline::context_t* pipeline_context) = 0;
    };

    using operator_merge_ptr = std::unique_ptr<operator_merge_t>;

    bool is_operator_merge(const components::expressions::compare_expression_ptr& expr);
    operator_merge_ptr create_operator_merge(context_collection_t* context, const components::expressions::compare_type& type, predicates::limit_t limit);
    operator_merge_ptr create_operator_merge(context_collection_t* context, const components::expressions::compare_expression_ptr& expr, predicates::limit_t limit);

} // namespace services::collection::operators::merge
