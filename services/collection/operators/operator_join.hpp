#pragma once

#include <collection/operators/operator.hpp>
#include <components/expressions/join_expression.hpp>
#include <components/ql/join/join.hpp>
#include <components/ql/join/join_types.h>

namespace services::collection::operators {

    class operator_join_t final : public read_only_operator_t {
    public:
        using type = components::ql::join_type;

        explicit operator_join_t(context_collection_t* context, const components::expressions::join_expression_t* expr);

    private:
        const components::expressions::join_expression_t* expr_;
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::collection::operators
