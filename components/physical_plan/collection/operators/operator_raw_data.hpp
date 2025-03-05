#pragma once

#include <components/document/document.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators {

    class operator_raw_data_t final : public read_only_operator_t {
    public:
        operator_raw_data_t(std::pmr::vector<components::document::document_ptr>&& documents);
        operator_raw_data_t(const std::pmr::vector<components::document::document_ptr>& documents);

    private:
        void on_execute_impl(components::pipeline::context_t*) final;
    };

} // namespace services::collection::operators
