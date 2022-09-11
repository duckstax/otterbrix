#include "operator_delete.hpp"

namespace services::collection::operators {

    operator_delete::operator_delete(context_collection_t* context)
        : operator_t(context, operator_type::remove) {
    }

    void operator_delete::on_execute_impl(components::cursor::sub_cursor_t* cursor) {
        if (cursor) {
            for (const auto& document : cursor->data()) {
                context_->storage().erase(context_->storage().find(document.id()));
            }
        }
    }

} // namespace services::collection::operators
