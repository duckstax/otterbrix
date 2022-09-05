#include "aggregation.hpp"

#include <collection/operators/predicates/limit.hpp>

namespace services::collection::operators {

    aggregation::aggregation(context_collection_t* ptr)
        : context_(ptr) {}

    void aggregation::on_execute(components::ql::find_statement& cond, components::cursor::sub_cursor_t* sub_cursor) {
        predicates::limit_t limit;
        if (cond.type() == components::ql::statement_type::find_one) {
            limit = predicates::limit_t(1);
        }
        auto predicate = create_predicate(cond);
        auto* ptr = new full_scan(context_);
        ptr->on_execute(predicate, limit, sub_cursor);
    }
} // namespace services::collection::operators
