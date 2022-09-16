#include "operator_insert.hpp"

namespace services::collection::operators {

    operator_insert::operator_insert(context_collection_t* context, std::pmr::vector<document_ptr>&& documents)
        : read_write_operator_t(context, operator_type::insert)
        , documents_(std::move(documents)) {
    }

    operator_insert::operator_insert(context_collection_t* context, const std::pmr::vector<document_ptr>& documents)
        : read_write_operator_t(context, operator_type::insert)
        , documents_(documents) {
    }

    void operator_insert::on_execute_impl(planner::transaction_context_t* transaction_context) {
        if (left_ && left_->output() && left_->output()->size() > 0) {
            //todo: error not unique keys
            return;
        }
        for (const auto &document : documents_) {
            auto id = get_document_id(document);
            context_->storage().insert_or_assign(id, document);
            context_->index_engine()->insert_document(document);
        }
    }

} // namespace services::collection::operators
