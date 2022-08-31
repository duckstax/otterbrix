#include "gt.hpp"

namespace services::collection::operators {

    gt::gt(const context_t& context, components::ql::key_t key, components::ql::expr_value_t value)
        : predicate(context)
        , key_(std::move(key))
        , value_(value) {}

    bool gt::check_impl(const components::document::document_ptr& document) {
        return get_value_from_document(document, key_) > value_;
    }

} // namespace services::operators
