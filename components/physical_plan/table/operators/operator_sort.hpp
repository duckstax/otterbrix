#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/physical_plan/table/operators/sort/sort.hpp>

namespace services::table::operators {

    class operator_sort_t final : public read_only_operator_t {
    public:
        using order = sort::order;

        explicit operator_sort_t(collection::context_collection_t* context);

        void add(size_t index, order order_ = order::ascending);
        void add(const std::vector<size_t>& indices, order order_ = order::ascending);

    private:
        sort::sorter_t sorter_;

        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;
    };

} // namespace services::table::operators
