#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/update_expression.hpp>

#include <components/physical_plan/base/operators/operator.hpp>

namespace components::table::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(services::collection::context_collection_t* context,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert,
                        expressions::compare_expression_ptr comp_expr = nullptr);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        std::pmr::vector<expressions::update_expr_ptr> updates_;
        expressions::compare_expression_ptr comp_expr_;
        bool upsert_;
    };

} // namespace components::table::operators
