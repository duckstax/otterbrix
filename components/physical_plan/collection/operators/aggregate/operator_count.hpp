#pragma once

#include <components/document/document.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_aggregate.hpp>

namespace services::collection::operators::aggregate {

    class operator_count_t final : public operator_aggregate_t {
    public:
        explicit operator_count_t(context_collection_t* collection);

    private:
        components::document::document_ptr aggregate_impl() final;
        std::string key_impl() const final;
    };

} // namespace services::collection::operators::aggregate