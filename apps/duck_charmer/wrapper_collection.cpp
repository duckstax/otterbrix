#include "wrapper_collection.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include "convert.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)


void wrapper_collection::insert(const py::handle &document) {
    auto is_document = py::isinstance<py::dict>(document);
    if (is_document) {
        auto doc = friedrichdb::core::make_document();
        to_document(document,*doc);
        ptr_->insert(document["_id"].cast<std::string>(), std::move(doc));
    }
}

wrapper_collection::~wrapper_collection() {
    ptr_.release();
}

wrapper_collection::wrapper_collection(wrapper_collection::pointer ptr) :ptr_(ptr) {}


auto wrapper_collection::get(py::object cond) -> py::object {
    auto is_not_empty = !cond.is(py::none());
    if (is_not_empty) {
        wrapper_document_ptr tmp;
        //std::cerr << ptr_->size() << std::endl;
        for (auto &i:*ptr_) {
            auto result = cache_.find(i.first);
            if (result == cache_.end()) {
                auto it = cache_.emplace(i.first, wrapper_document_ptr(new wrapper_document(i.second.get())));
                tmp = it.first->second;
            } else {
                tmp = result->second;
            }
            if (cond(tmp).cast<bool>()) {
                return py::cast(tmp);
            }
        }

        return py::none();

    }

}


auto wrapper_collection::search(py::object cond) -> py::list {
    py::list tmp;
    wrapper_document_ptr doc;
    for (auto &i:*ptr_) {
        auto result = cache_.find(i.first);
        if (result == cache_.end()) {
            auto it = cache_.emplace(i.first, wrapper_document_ptr(new wrapper_document(i.second.get())));
            doc = it.first->second;
        } else {
            doc = result->second;
        }
        if (cond(doc).cast<bool>()) {
            tmp.append(doc);
        }
    }
    return tmp;
}

auto wrapper_collection::all() -> py::list {
    py::list tmp;
    wrapper_document_ptr doc;
    for (auto &i:*ptr_) {
        auto result = cache_.find(i.first);
        if (result == cache_.end()) {
            auto it = cache_.emplace(i.first, wrapper_document_ptr(new wrapper_document(i.second.get())));
            doc = it.first->second;
        } else {
            doc = result->second;
        }

        tmp.append(doc);
    }
    return tmp;
}

void wrapper_collection::insert_many(py::iterable iterable) {
    auto iter = py::iter(iterable);
    for(;iter!= py::iterator::sentinel();++iter) {
        auto document = *iter;
        auto is_document = py::isinstance<py::dict>(document);
        if (is_document) {
            auto doc = friedrichdb::core::make_document();
            to_document(document,*doc);
            ptr_->insert(document["_id"].cast<std::string>(), std::move(doc));
        }
    }
}

std::size_t wrapper_collection::size() const {
    return ptr_->size();
}

void wrapper_collection::update(py::dict fields, py::object cond) {
    auto is_document = py::isinstance<py::dict>(fields);
    auto is_none = fields.is(py::none());
    if (is_none and is_document) {
        throw pybind11::type_error("fields is none or not dict  ");
    }

    auto is_not_none_cond = !cond.is(py::none());

    if (is_not_none_cond) {
        wrapper_document_ptr doc;
        for (auto &i:*ptr_) {
            auto result = cache_.find(i.first);
            if (result == cache_.end()) {
                auto it = cache_.emplace(i.first, wrapper_document_ptr(new wrapper_document(i.second.get())));
                doc = it.first->second;
            } else {
                doc = result->second;
            }

            update_document(fields, *(doc->raw()));
        }
        return;
    }

    throw pybind11::type_error(" note cond ");

}

void wrapper_collection::remove(py::object cond) {
    auto is_not_empty = !cond.is(py::none());
    if (is_not_empty) {
        wrapper_document_ptr tmp;
        std::string key;
        for (auto &i:*ptr_) {
            auto result = cache_.find(i.first);
            if (result == cache_.end()) {
                auto it = cache_.emplace(i.first, wrapper_document_ptr(new wrapper_document(i.second.get())));
                tmp = it.first->second;
                key = it.first->first;
            } else {
                tmp = result->second;
                key= result->first;
            }
            if (cond(tmp).cast<bool>()) {
               cache_.erase(key);
               ptr_->remove(key);
            }
        }
    }
}

void wrapper_collection::drop() {
    cache_.clear();
    ptr_->drop();
}

/*

void wrapper_collection::update(py::dict fields, py::object cond, py::iterable doc_ids) {
    std::cerr << "void wrapper_collection::update(py::dict fields, py::object cond, py::iterable doc_ids)" << std::endl;
    auto is_document = py::isinstance<py::dict>(fields);
    auto is_none = fields.is(py::none());
    if (is_none and is_document) {
        throw pybind11::type_error("fields is none or not dict  ");
    }

    auto is_not_none_doc_ids = !doc_ids.is(py::none());

    if (is_not_none_doc_ids) {
        auto iter = py::iter(doc_ids);
        for (; iter != py::iterator::sentinel(); ++iter) {
            auto id = *iter;
            auto uid = id.cast<std::string>();
            wrapper_document_ptr doc_ptr;
            auto it = cache_.find(uid);
            if (it == cache_.end()) {
                auto *doc_raw_ptr = ptr_->get(uid);
                if (doc_raw_ptr == nullptr) {
                    std::cerr << "null ptr" << std::endl;
                } else {
                    auto result = cache_.emplace(uid, wrapper_document_ptr(new wrapper_document(doc_raw_ptr)));
                    doc_ptr = result.first->second;
                }
            } else {
                doc_ptr = it->second;
            }

            update_document(fields, *(doc_ptr->raw()));

        }
        return;
    }

    auto is_not_none_cond = !doc_ids.is(py::none());

    if (is_not_none_cond) {
        wrapper_document_ptr doc;
        for (auto &i:*ptr_) {
            auto result = cache_.find(i.first);
            if (result == cache_.end()) {
                auto it = cache_.emplace(i.first, wrapper_document_ptr(new wrapper_document(i.second.get())));
                doc = it.first->second;
            } else {
                doc = result->second;
            }

            update_document(fields, *(doc->raw()));
        }
        return;
    }

    throw pybind11::type_error(" note cond or cond  ");

}

 */