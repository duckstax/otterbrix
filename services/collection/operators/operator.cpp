#include "operator.hpp"

namespace services::collection::operators {

    operator_t::operator_t(services::collection::collection_t* collection)
        : collection_(collection->view()) {
    }

    void operator_t::on_execute(predicate_ptr predicate,limit_t limit,components::cursor::sub_cursor_t* cursor){
        return on_execute_impl(predicate,limit,cursor);
    }

} // namespace services::operators
