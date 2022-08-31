#include "scanner.hpp"

namespace services::collection::operators {

    scanner::scanner(const context_t &context, services::collection::collection_t* collection)
        : context_(context)
        , collection_(collection) {
    }

    result_find scanner::scan(components::ql::find_statement& cond) {
        if (cond.type() == components::ql::statement_type::find_one) {
            return scan_one_impl(cond);
        }
        return scan_impl(cond);
    }

} // namespace services::operators
