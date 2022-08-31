#pragma once

#include <components/document/document.hpp>
#include <components/document/wrapper_value.hpp>
#include <components/ql/expr.hpp>
#include <services/collection/context.hpp>

namespace services::collection::operators {

    using services::collection::context_t;

    class predicate {
    public:
        explicit predicate(const context_t& context);
        predicate(const predicate&) = delete;
        predicate& operator=(const predicate&) = delete;
        virtual ~predicate() = default;

        bool check(const components::document::document_ptr& document);

    protected:
        context_t context_;

    private:
        virtual bool check_impl(const components::document::document_ptr& document) = 0;
    };

    using predicate_ptr = std::unique_ptr<predicate>;

    document::wrapper_value_t get_value_from_document(const components::document::document_ptr& document, const components::ql::key_t& key);

} // namespace services::operators
