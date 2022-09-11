#include "operator_update.hpp"

namespace services::collection::operators {

    operator_update::operator_update(context_collection_t* context, document_ptr update)
        : operator_t(context, operator_type::update)
        , update_(std::move(update)) {
    }

    void operator_update::on_execute_impl(components::cursor::sub_cursor_t* cursor) {
        if (cursor) {
            for (const auto& document_view : cursor->data()) {
                auto document = context_->storage().find(document_view.id());
                if (document != context_->storage().end() && document->second->update(*update_)) {
                    document->second->commit();
                }
            }
        }
        //todo: remake indexes
    }

} // namespace services::collection::operators
