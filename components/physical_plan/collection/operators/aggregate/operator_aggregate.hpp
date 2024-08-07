#pragma once

#include <components/document/document.hpp>
#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators::aggregate {

    class operator_aggregate_t : public read_only_operator_t {
    public:
        void set_value(document_ptr& doc, std::string_view key) const;
        components::document::value_t value() const;

    protected:
        explicit operator_aggregate_t(context_collection_t* collection);

    private:
        void on_execute_impl(components::pipeline::context_t* pipeline_context) final;

        virtual components::document::document_ptr aggregate_impl() = 0;
        virtual std::string key_impl() const = 0;
    };

    using operator_aggregate_ptr = boost::intrusive_ptr<operator_aggregate_t>;

} // namespace services::collection::operators::aggregate