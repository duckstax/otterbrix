#include "operator_and.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators::merge {

    operator_and_t::operator_and_t(context_collection_t* context, components::logical_plan::limit_t limit)
        : operator_merge_t(context, limit) {}

    void operator_and_t::on_merge_impl(components::pipeline::context_t*) {
        //todo: optimize merge
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }
        if (left_ && right_ && left_->output() && right_->output()) {
            output_ = base::operators::make_operator_data(left_->output()->resource());
            const auto& right_documents = std::get<std::pmr::vector<document_ptr>>(right_->output()->data());
            for (const auto& left_document : std::get<std::pmr::vector<document_ptr>>(left_->output()->data())) {
                auto it = std::find_if(right_documents.cbegin(),
                                       right_documents.cend(),
                                       [&left_document](const document_ptr& doc) {
                                           return get_document_id(doc) == get_document_id(left_document);
                                       });
                if (it != right_documents.cend()) {
                    output_->append(left_document);
                    ++count;
                    if (!limit_.check(count)) {
                        return;
                    }
                }
            }
        }
    }

} // namespace services::collection::operators::merge
