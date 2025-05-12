#pragma once

#include <components/document/document.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace services::collection::operators {

    class operator_insert final : public read_write_operator_t {
    public:
        operator_insert(
            context_collection_t* collection,
            std::pmr::vector<std::pair<components::expressions::key_t, components::expressions::key_t>> = {});

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        std::pmr::vector<std::pair<components::expressions::key_t, components::expressions::key_t>> key_translation_;
    };

} // namespace services::collection::operators
