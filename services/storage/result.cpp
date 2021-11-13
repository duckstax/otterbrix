#include "result.hpp"

result_find::result_find(result_t &&finded_docs)
    : finded_docs_(std::move(finded_docs))
{}

const result_find::result_t &result_find::operator *() const {
    return finded_docs_;
}

result_find::result_t *result_find::operator ->() {
    return &finded_docs_;
}


result_size::result_size(std::size_t size) :
    size_(size)
{}

std::size_t result_size::operator *() const {
    return size_;
}

result_get_document::result_get_document(result_get_document::result_t &&doc)
    : doc_(std::move(doc)) {
}

const result_get_document::result_t &result_get_document::operator *() const {
    return doc_;
}

result_get_document::result_t *result_get_document::operator ->() {
    return &doc_;
}
