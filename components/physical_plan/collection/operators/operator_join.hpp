#pragma once

#include "predicates/predicate.hpp"

#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_join_t final : public read_only_operator_t {
    public:
        using type = components::logical_plan::join_type;

        explicit operator_join_t(context_collection_t* context, type join_type, predicates::predicate_ptr&& predicate);

    private:
        type join_type_;
        predicates::predicate_ptr predicate_;

        bool check_expressions_(const components::document::document_ptr& left,
                                const components::document::document_ptr& right,
                                components::pipeline::context_t* context);
        void on_execute_impl(components::pipeline::context_t* context) final;
        void inner_join_(components::pipeline::context_t* context);
        void outer_full_join_(components::pipeline::context_t* context);
        void outer_left_join_(components::pipeline::context_t* context);
        void outer_right_join_(components::pipeline::context_t* context);
        void cross_join_(components::pipeline::context_t* context);
    };

} // namespace services::collection::operators
