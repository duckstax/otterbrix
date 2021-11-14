#include "result.hpp"

result_insert_one::result_insert_one(result_insert_one::result_t count_inserted)
    : count_inserted_(count_inserted)
{}

result_insert_one::result_t result_insert_one::operator *() const {
    return count_inserted_;
}

result_insert_one::result_t result_insert_one::inserted() const {
    return count_inserted_;
}


result_insert_many::result_insert_many(result_insert_many::result_t count_inserted, result_insert_many::result_t count_not_inserted)
    : count_inserted_(count_inserted)
    , count_not_inserted_(count_not_inserted) {
}

std::pair<result_insert_many::result_t, result_insert_many::result_t> result_insert_many::operator *() const {
    return { count_inserted_, count_not_inserted_ };
}

result_insert_many::result_t result_insert_many::inserted() const {
    return count_inserted_;
}

result_insert_many::result_t result_insert_many::not_inserted() const {
    return count_not_inserted_;
}


result_find::result_find(result_t &&finded_docs)
    : finded_docs_(std::move(finded_docs))
{}

const result_find::result_t &result_find::operator *() const {
    return finded_docs_;
}

result_find::result_t *result_find::operator ->() {
    return &finded_docs_;
}


result_size::result_size(result_t size) :
    size_(size)
{}

result_size::result_t result_size::operator *() const {
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
