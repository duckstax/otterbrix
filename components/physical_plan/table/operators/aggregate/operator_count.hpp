#pragma once

#include "operator_aggregate.hpp"

namespace components::table::operators::aggregate {

    class operator_count_t final : public operator_aggregate_t {
    public:
        explicit operator_count_t(services::collection::context_collection_t* collection);

    private:
        types::logical_value_t aggregate_impl() final;
        std::string key_impl() const final;
    };

} // namespace components::table::operators::aggregate