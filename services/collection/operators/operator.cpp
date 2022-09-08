#include "operator.hpp"

namespace services::collection::operators {

    operator_t::operator_t(context_collection_t* context, operator_type type)
        : context_(context)
        , operator_type_(type) {
    }

    void operator_t::on_execute(components::cursor::sub_cursor_t* cursor) {
        return on_execute_impl(cursor);
    }

} // namespace services::collection::operators
