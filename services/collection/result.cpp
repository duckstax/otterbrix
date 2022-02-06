#include "result.hpp"

result_insert_one::result_insert_one(result_insert_one::result_t id)
    : inserted_id_(id) {}

const result_insert_one::result_t& result_insert_one::inserted_id() const {
    return inserted_id_;
}

result_insert_many::result_insert_many(result_t&& inserted_ids)
    : inserted_ids_(std::move(inserted_ids)) {
}

const result_insert_many::result_t& result_insert_many::inserted_ids() const {
    return inserted_ids_;
}

result_find::result_find(result_t&& finded_docs)
    : finded_docs_(std::move(finded_docs)) {}

const result_find::result_t& result_find::operator*() const {
    return finded_docs_;
}

result_find::result_t* result_find::operator->() {
    return &finded_docs_;
}

result_find_one::result_find_one(const result_find_one::result_t& finded_doc)
    : finded_doc_(finded_doc)
    , is_find_(true) {
}

bool result_find_one::is_find() const {
    return is_find_;
}

const result_find_one::result_t& result_find_one::operator*() const {
    return finded_doc_;
}

result_find_one::result_t* result_find_one::operator->() {
    return &finded_doc_;
}

result_size::result_size(result_t size)
    : size_(size) {}

result_size::result_t result_size::operator*() const {
    return size_;
}

result_get_document::result_get_document(result_get_document::result_t&& doc)
    : doc_(std::move(doc)) {
}

const result_get_document::result_t& result_get_document::operator*() const {
    return doc_;
}

result_get_document::result_t* result_get_document::operator->() {
    return &doc_;
}

result_drop_collection::result_drop_collection(result_drop_collection::result_t success)
    : success_(success) {}

result_drop_collection::result_t result_drop_collection::is_success() const {
    return success_;
}

result_delete::result_delete(result_delete::result_t&& deleted_ids)
    : deleted_ids_(std::move(deleted_ids)) {
}

const result_delete::result_t& result_delete::deleted_ids() const {
    return deleted_ids_;
}

result_update::result_update(result_update::result_t&& modified_ids, result_update::result_t&& nomodified_ids)
    : modified_ids_(std::move(modified_ids))
    , nomodified_ids_(std::move(nomodified_ids)) {
}

result_update::result_update(std::string&& upserted_id)
    : upserted_id_(std::move(upserted_id)) {
}

const result_update::result_t& result_update::modified_ids() const {
    return modified_ids_;
}

const result_update::result_t& result_update::nomodified_ids() const {
    return nomodified_ids_;
}

const std::string& result_update::upserted_id() const {
    return upserted_id_;
}
