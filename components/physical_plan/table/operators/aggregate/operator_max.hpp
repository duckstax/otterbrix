#pragma once

#include "operator_aggregate.hpp"
#include <components/index/index.hpp>

namespace services::table::operators::aggregate {

    class operator_max_t final : public operator_aggregate_t {
    public:
        explicit operator_max_t(collection::context_collection_t* collection, components::index::key_t key);

    private:
        components::index::key_t key_;

        components::types::logical_value_t aggregate_impl() final;
        std::string key_impl() const final;
    };

} // namespace services::table::operators::aggregate