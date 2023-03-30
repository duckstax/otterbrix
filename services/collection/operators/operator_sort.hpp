#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/sort.hpp>

namespace services::collection::operators {

    class operator_sort_t final : public read_only_operator_t {
    public:
        using order = services::storage::sort::order;

        explicit operator_sort_t(context_collection_t* context);

        void add(const std::string& key, order order_ = order::ascending);
        void add(const std::vector<std::string>& keys, order order_ = order::ascending);

    private:
        services::storage::sort::sorter_t sorter_;

        void on_execute_impl(components::transaction::context_t* transaction_context) final;
    };

} // namespace services::collection::operators
