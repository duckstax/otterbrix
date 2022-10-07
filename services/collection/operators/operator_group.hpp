#pragma once

#include <services/collection/operators/operator.hpp>
#include <services/collection/operators/get/operator_get.hpp>

namespace services::collection::operators {

    struct group_key_t {
        std::string name;
        get::operator_get_ptr getter;
    };

    class operator_group_t final : public read_write_operator_t {
    public:
        explicit operator_group_t(context_collection_t* context);

        void add_key(const std::string &name, get::operator_get_ptr &&getter);

    private:
        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<operator_data_ptr> input_documents_;

        void on_execute_impl(planner::transaction_context_t* transaction_context) final;

        void create_list_documents();
    };

} // namespace services::collection::operators