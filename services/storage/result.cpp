#include "result.hpp"

result_find::result_find(std::vector<components::storage::document_t *> &&finded_docs)
    : finded_docs_(std::move(finded_docs))
{}

const std::vector<components::storage::document_t *> &result_find::operator *() const {
    return finded_docs_;
}


result_size::result_size(std::size_t size) :
    size_(size)
{}

std::size_t result_size::operator *() const {
    return size_;
}
