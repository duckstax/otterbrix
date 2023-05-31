#pragma once

#include <components/document/document.hpp>
#include <components/index/index.hpp>
#include <services/collection/operators/aggregate/operator_aggregate.hpp>

namespace services::collection::operators::aggregate {

    class operator_avg_t final : public operator_aggregate_t {
    public:
        explicit operator_avg_t(context_collection_t *collection, components::index::key_t key);

    private:
        components::index::key_t key_;

        components::document::document_ptr aggregate_impl() final;
        std::string key_impl() const final;
    };

} // namespace services::operators::aggregate