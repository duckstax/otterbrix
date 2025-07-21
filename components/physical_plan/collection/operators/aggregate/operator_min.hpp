#pragma once

#include <components/document/document.hpp>
#include <components/index/index.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_aggregate.hpp>

namespace components::collection::operators::aggregate {

    class operator_min_t final : public operator_aggregate_t {
    public:
        explicit operator_min_t(services::collection::context_collection_t* collection, index::key_t key);

    private:
        index::key_t key_;

        document::document_ptr aggregate_impl() final;
        std::string key_impl() const final;
    };

} // namespace components::collection::operators::aggregate