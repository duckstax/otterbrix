#pragma once

#include "operator.hpp"

namespace services::base::operators {

    class aggregation final : public read_only_operator_t {
    public:
        explicit aggregation(collection::context_collection_t* context);

        void set_match(operator_ptr&& match);
        void set_group(operator_ptr&& group);
        void set_sort(operator_ptr&& sort);

    private:
        operator_ptr match_{nullptr};
        operator_ptr group_{nullptr};
        operator_ptr sort_{nullptr};

        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
        void on_prepare_impl() final;
    };

} // namespace services::base::operators
