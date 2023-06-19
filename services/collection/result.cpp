#include "result.hpp"


result_insert::result_insert(std::pmr::memory_resource *resource)
    : inserted_ids_(resource) {}

result_insert::result_insert(result_t&& inserted_ids)
    : inserted_ids_(std::move(inserted_ids)) {
}

const result_insert::result_t& result_insert::inserted_ids() const {
    return inserted_ids_;
}

bool result_insert::empty() const {
    return inserted_ids_.empty();
}


result_find::result_find(std::pmr::memory_resource *resource)
    : finded_docs_(resource) {}

result_find::result_find(result_t&& finded_docs)
    : finded_docs_(std::move(finded_docs)) {}

const result_find::result_t& result_find::operator*() const {
    return finded_docs_;
}

result_find::result_t* result_find::operator->() {
    return &finded_docs_;
}


result_find_one::result_find_one(result_t&& finded_doc)
    : finded_doc_(std::move(finded_doc))
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


result_drop_collection::result_drop_collection(result_drop_collection::result_t success)
    : success_(success) {}

result_drop_collection::result_t result_drop_collection::is_success() const {
    return success_;
}


result_delete::result_delete(std::pmr::memory_resource *resource)
    : deleted_ids_(resource) {}

result_delete::result_delete(result_delete::result_t&& deleted_ids)
    : deleted_ids_(std::move(deleted_ids)) {
}

const result_delete::result_t& result_delete::deleted_ids() const {
    return deleted_ids_;
}

bool result_delete::empty() const {
    return deleted_ids_.empty();
}


result_update::result_update(std::pmr::memory_resource *resource)
    : modified_ids_(resource)
    , nomodified_ids_(resource)
    , upserted_id_(document_id_t::null()) {
}

result_update::result_update(result_update::result_t&& modified_ids, result_update::result_t&& nomodified_ids)
    : modified_ids_(std::move(modified_ids))
    , nomodified_ids_(std::move(nomodified_ids))
    , upserted_id_(document_id_t::null()) {
}

result_update::result_update(const result_t &modified_ids, const result_t &nomodified_ids)
    : modified_ids_(modified_ids)
    , nomodified_ids_(nomodified_ids)
    , upserted_id_(document_id_t::null()) {
}

result_update::result_update(const document_id_t& upserted_id, std::pmr::memory_resource *resource)
    : modified_ids_(resource)
    , nomodified_ids_(resource)
    , upserted_id_(upserted_id) {
}

const result_update::result_t& result_update::modified_ids() const {
    return modified_ids_;
}

const result_update::result_t& result_update::nomodified_ids() const {
    return nomodified_ids_;
}

const result_update::document_id_t& result_update::upserted_id() const {
    return upserted_id_;
}

bool result_update::empty() const {
    return modified_ids_.empty() && upserted_id().is_null();
}


result_create_index::result_create_index(result_create_index::result_t success)
    : success_(success) {
}

result_create_index::result_t result_create_index::is_success() const {
    return success_;
}


result_drop_index::result_drop_index(result_drop_index::result_t success)
    : success_(success) {
}

result_drop_index::result_t result_drop_index::is_success() const {
    return success_;
}
