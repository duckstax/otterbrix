#include "operator_sort.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    operator_sort_t::operator_sort_t(context_collection_t* context)
        : read_only_operator_t(context, operator_type::sort) {}

    void operator_sort_t::add(const std::string& key, operator_sort_t::order order_) { sorter_.add(key, order_); }

    void operator_sort_t::add(const std::vector<std::string>& keys, order order_) {
        for (const auto& key : keys) {
            sorter_.add(key, order_);
        }
    }

    void operator_sort_t::on_execute_impl(components::pipeline::context_t*) {
        if (left_ && left_->output()) {
            output_ = make_operator_data(left_->output()->resource());
            for (const auto& document : left_->output()->documents()) {
                output_->append(document);
            }
            auto& documents = output_->documents();
            std::sort(documents.begin(), documents.end(), sorter_);
        }
    }

} // namespace services::collection::operators
