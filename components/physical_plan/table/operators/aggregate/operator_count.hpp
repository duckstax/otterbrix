#pragma once

#include "operator_aggregate.hpp"

namespace services::table::operators::aggregate {

    class operator_count_t final : public operator_aggregate_t {
    public:
        explicit operator_count_t(collection::context_collection_t* collection);

    private:
        components::types::logical_value_t aggregate_impl() final;
        std::string key_impl() const final;
    };

} // namespace services::table::operators::aggregate