#pragma once

#include "operator_merge.hpp"

namespace services::collection::operators::merge {

    class operator_not_t : public operator_merge_t {
    public:
        explicit operator_not_t(context_collection_t* context, components::logical_plan::limit_t limit);

    private:
        void on_merge_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::collection::operators::merge
