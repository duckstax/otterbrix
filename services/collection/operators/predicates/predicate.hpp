#pragma once

#include <services/collection/collection.hpp>
#include <components/document/document.hpp>
#include <components/document/wrapper_value.hpp>
#include <components/ql/expr.hpp>
#include <components/ql/find.hpp>

namespace services::collection::operators::predicates {

    using services::collection::context_collection_t;

    class predicate {
    public:
        explicit predicate(context_collection_t* context);
        predicate(const predicate&) = delete;
        predicate& operator=(const predicate&) = delete;
        virtual ~predicate() = default;

        bool check(const components::document::document_ptr& document);

    protected:
        context_collection_t* context_;

    private:
        virtual bool check_impl(const components::document::document_ptr& document) = 0;
    };

    using predicate_ptr = std::unique_ptr<predicate>;

    document::wrapper_value_t get_value_from_document(const components::document::document_ptr& document, const components::ql::key_t& key);

    predicate_ptr create_predicate(context_collection_t* context, const components::ql::find_statement_ptr& cond);

} // namespace services::collection::operators::predicates
