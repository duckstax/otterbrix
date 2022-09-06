#include "insert.hpp"
#include <services/collection/operators/predicates/gt.hpp>

#include <services/collection/collection.hpp>

namespace services::collection::operators {

    insert::insert(context_collection_t* context)
        : operator_t(context) {
    }

    void insert::on_execute_impl(const predicate_ptr& predicate, predicates::limit_t limit, components::cursor::sub_cursor_t* cursor) {
        document_view_t view(document);
        auto id = document_id_t(view.get_string("_id"));
        if (context_->storage().contains(id)) {
            //todo error primary key
        } else {
            context_->storage().insert_or_assign(id, document);
            return id;
        }
        return document_id_t::null();
    }

} // namespace services::collection::operators
