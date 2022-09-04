#include "predicate.hpp"
#include <components/document/document_view.hpp>
#include "gt.hpp"

namespace services::collection::operators {

    using components::ql::condition_type;

    predicate::predicate(context_collection_t* context)
        : context_(context) {}

    bool predicate::check(const components::document::document_ptr& document) {
        return check_impl(document);
    }

    document::wrapper_value_t get_value_from_document(const components::document::document_ptr& document, const components::ql::key_t& key) {
        return document::wrapper_value_t(components::document::document_view_t(document).get_value(key.as_string()));
    }

    predicate_ptr create_predicate(context_collection_t* context, components::ql::find_statement& cond) {
        switch (cond.condition_->type_) {
            case condition_type::gt:
                return std::make_unique<gt>(context, cond.condition_->key_, cond.condition_->value_);
            default:
                break;
        }
        static_assert(true, "not valid condition type");
        return nullptr;
    }

} // namespace services::operators
