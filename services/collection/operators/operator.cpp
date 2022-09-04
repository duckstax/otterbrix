#include "operator.hpp"

namespace services::collection::operators {

    operator_t::operator_t(services::collection::collection_t* collection)
        : collection_(collection->view()) {
    }

    result_find operator_t::scan(components::ql::find_statement& cond) {
        if (cond.type() == components::ql::statement_type::find_one) {
            return scan_one_impl(cond);
        }
        return scan_impl(cond);
    }

} // namespace services::operators
