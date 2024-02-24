#pragma once

#include <collection/operators/operator.hpp>
#include <components/expressions/join_expression.hpp>
#include <components/ql/join/join.hpp>
#include <components/ql/join/join_types.h>

namespace services::collection::operators {

    class operator_join_t final : public read_only_operator_t {
    public:
        using type = components::ql::join_type;

        explicit operator_join_t(context_collection_t* context, type join_type, std::pmr::vector<components::expressions::join_expression_ptr>&& expressions);

    private:
        type join_type_;
        std::pmr::vector<components::expressions::join_expression_ptr> expressions_;

        bool check_expressions_(components::document::document_view_t left, components::document::document_view_t right);
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
        void inner_join_();
        void outer_full_join_();
        void outer_left_join_();
        void outer_right_join_();
        void cross_join_();
    };

} // namespace services::collection::operators
