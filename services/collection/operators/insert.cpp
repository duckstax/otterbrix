#include "insert.hpp"

namespace services::collection::operators {

    insert::insert(context_collection_t* context, std::list<document_ptr>&& documents)
        : operator_t(context, operator_type::insert)
        , documents_(std::move(documents)) {
    }

    insert::insert(context_collection_t* context, const std::list<document_ptr>& documents)
        : operator_t(context, operator_type::insert)
        , documents_(documents) {
    }

    void insert::on_execute_impl(components::cursor::sub_cursor_t* cursor) {
        if (cursor && cursor->size() > 0) {
            //todo: error not unique keys
            return;
        }
        for (const auto &document : documents_) {
            auto id = get_document_id(document);
            context_->storage().insert_or_assign(id, document);
        }
        //todo: remake indexes
    }

} // namespace services::collection::operators
