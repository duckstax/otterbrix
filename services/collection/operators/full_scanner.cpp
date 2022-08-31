#include "full_scanner.hpp"
#include <services/collection/operators/predicates/gt.hpp>

namespace services::collection::operators {

    using components::ql::condition_type;

    predicate_ptr create_predicate(const context_t& context, components::ql::find_statement& cond) {
        switch (cond.condition_->type_) {
            case condition_type::gt:
                return std::make_unique<gt>(context, cond.condition_->key_, cond.condition_->value_);
            default:
                break;
        }
        static_assert(true, "not valid condition type");
        return nullptr;
    }


    full_scanner::full_scanner(const context_t &context, services::collection::collection_t* collection)
        : scanner(context, collection) {
    }

    result_find full_scanner::scan_impl(components::ql::find_statement& cond) {
        result_find::result_t result;
        auto predicate = create_predicate(context_, cond);
        for (auto& it : collection_->storage()) {
            if (predicate->check(it.second)) {
                result.push_back(document_view_t(it.second));
            }
        }
        return result_find(std::move(result));
    }

    result_find full_scanner::scan_one_impl(components::ql::find_statement& cond) {
        auto predicate = create_predicate(context_, cond);
        for (auto& it : collection_->storage()) {
            if (predicate->check(it.second)) {
                result_find({document_view_t(it.second)});
            }
        }
        return result_find();
    }

} // namespace services::collection::operators
