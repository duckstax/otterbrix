#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators::aggregate {

    class operator_aggregate_t : public read_only_operator_t {
    public:
        document::wrapper_value_t value() const;

    protected:
        explicit operator_aggregate_t(context_collection_t* collection);

    private:
        void on_execute_impl(components::transaction::context_t* transaction_context) final;

        virtual document_ptr aggregate_impl() = 0;
        virtual std::string key_impl() const = 0;
    };

    using operator_aggregate_ptr = std::unique_ptr<operator_aggregate_t>;

} // namespace services::operators::aggregate