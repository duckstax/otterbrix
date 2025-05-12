#pragma once

#include <components/document/document.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace services::collection::operators::predicates {

    class predicate : public boost::intrusive_ref_counter<predicate> {
    public:
        predicate() = default;
        predicate(const predicate&) = delete;
        predicate& operator=(const predicate&) = delete;
        virtual ~predicate() = default;

        bool check(const components::document::document_ptr& document,
                   const components::logical_plan::storage_parameters* parameters);
        bool check(const components::document::document_ptr& document_left,
                   const components::document::document_ptr& document_right,
                   const components::logical_plan::storage_parameters* parameters);

    private:
        virtual bool check_impl(const components::document::document_ptr& document_left,
                                const components::document::document_ptr& document_right,
                                const components::logical_plan::storage_parameters* parameters) = 0;
    };

    using predicate_ptr = boost::intrusive_ptr<predicate>;

    predicate_ptr create_predicate(const components::expressions::compare_expression_ptr& expr);

    predicate_ptr create_all_true_predicate(std::pmr::memory_resource* resource);

} // namespace services::collection::operators::predicates
