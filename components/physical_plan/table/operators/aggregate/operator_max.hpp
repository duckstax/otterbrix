#pragma once

#include "operator_aggregate.hpp"
#include <components/index/index.hpp>

namespace components::table::operators::aggregate {

    class operator_max_t final : public operator_aggregate_t {
    public:
        explicit operator_max_t(services::collection::context_collection_t* collection, index::key_t key);

    private:
        index::key_t key_;

        types::logical_value_t aggregate_impl() final;
        std::string key_impl() const final;
    };

} // namespace components::table::operators::aggregate