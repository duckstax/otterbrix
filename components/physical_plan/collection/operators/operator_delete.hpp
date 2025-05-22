#pragma once

#include <components/physical_plan/collection/operators/operator.hpp>

#include "predicates/predicate.hpp"

namespace services::collection::operators {

    class operator_delete final : public read_write_operator_t {
    public:
        explicit operator_delete(context_collection_t* collection,
                                 predicates::predicate_ptr&& match_predicate = nullptr);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        predicates::predicate_ptr match_predicate_;
    };

} // namespace services::collection::operators