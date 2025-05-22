#pragma once

#include <components/document/document.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

#include "predicates/predicate.hpp"

namespace services::collection::operators {

    class operator_update final : public read_write_operator_t {
    public:
        operator_update(context_collection_t* context,
                        std::pmr::vector<components::expressions::update_expr_ptr> updates,
                        bool upsert,
                        predicates::predicate_ptr&& match_predicate = nullptr);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        predicates::predicate_ptr match_predicate_;
        std::pmr::vector<components::expressions::update_expr_ptr> updates_;
        bool upsert_;
    };

} // namespace services::collection::operators
