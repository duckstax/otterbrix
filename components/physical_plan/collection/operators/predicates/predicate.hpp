#pragma once

#include <components/document/document.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <services/collection/collection.hpp>

namespace services::collection::operators::predicates {

    using services::collection::context_collection_t;

    class predicate : public boost::intrusive_ref_counter<predicate> {
    public:
        explicit predicate(context_collection_t* context);
        predicate(const predicate&) = delete;
        predicate& operator=(const predicate&) = delete;
        virtual ~predicate() = default;

        bool check(const components::document::document_ptr& document,
                   const components::logical_plan::storage_parameters* parameters);
        bool check(const components::document::document_ptr& document_left,
                   const components::document::document_ptr& document_right,
                   const components::logical_plan::storage_parameters* parameters);

    protected:
        context_collection_t* context_;

    private:
        virtual bool check_impl(const components::document::document_ptr& document_left,
                                const components::document::document_ptr& document_right,
                                const components::logical_plan::storage_parameters* parameters) = 0;
    };

    using predicate_ptr = boost::intrusive_ptr<predicate>;

    predicate_ptr create_predicate(context_collection_t* context,
                                   const components::expressions::compare_expression_ptr& expr);

} // namespace services::collection::operators::predicates
