#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/get/operator_get.hpp>
#include <services/collection/operators/aggregate/operator_aggregate.hpp>

namespace services::collection::operators {

    struct group_key_t {
        std::string name;
        get::operator_get_ptr getter;
    };

    struct group_value_t {
        std::string name;
        aggregate::operator_aggregate_ptr aggregator;
    };

    class operator_group_t final : public read_write_operator_t {
    public:
        explicit operator_group_t(context_collection_t* context);

        void add_key(const std::string &name, get::operator_get_ptr &&getter);
        void add_value(const std::string &name, aggregate::operator_aggregate_ptr &&aggregator);

    private:
        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<group_value_t> values_;
        std::pmr::vector<operator_data_ptr> input_documents_;

        void on_execute_impl(components::transaction::context_t* transaction_context) final;

        void create_list_documents();
        void calc_aggregate_values(components::transaction::context_t* transaction_context);
    };

} // namespace services::collection::operators