#include "sort.hpp"

namespace services::storage::sort {

sorter_t::sorter_t(const std::string &key, order order_) {
    add(key, order_);
}

void sorter_t::add(const std::string &key, order order_) {
    functions_.emplace_back([key, order_](const document_view_t *doc1, const document_view_t *doc2) {
        return (order_ == order::ascending ? 1 : -1) * doc1->equals(*doc2, key);
    });
}

}