#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

namespace services::table::operators::aggregate {

    class operator_aggregate_t : public read_only_operator_t {
    public:
        void set_value(std::pmr::vector<components::types::logical_value_t>& row, std::string_view key) const;
        components::types::logical_value_t value() const;

    protected:
        explicit operator_aggregate_t(collection::context_collection_t* collection);

        std::pmr::vector<components::types::logical_value_t> aggregate_result_;

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        virtual components::types::logical_value_t aggregate_impl() = 0;
        virtual std::string key_impl() const = 0;
    };

    using operator_aggregate_ptr = boost::intrusive_ptr<operator_aggregate_t>;

} // namespace services::table::operators::aggregate