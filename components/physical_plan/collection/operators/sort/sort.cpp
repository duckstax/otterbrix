#include "sort.hpp"

namespace services::collection::sort {

    sorter_t::sorter_t(const std::string& key, order order_) { add(key, order_); }

    void sorter_t::add(const std::string& key, order order_) {
        functions_.emplace_back([key, order_](const document_ptr& doc1, const document_ptr& doc2) {
            auto k_order = static_cast<int>(order_ == order::ascending ? compare_t::more : compare_t::less);
            return static_cast<compare_t>(k_order * static_cast<int>(doc1->compare(key, doc2, key)));
        });
    }

} // namespace services::collection::sort