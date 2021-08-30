#include "collection.hpp"
#include "database.hpp"

#include "protocol/insert.hpp"
#include "protocol/request_select.hpp"

namespace services::storage {
    collection_t::collection_t(database_ptr database, log_t& log)
        : goblin_engineer::abstract_service(database, "collection")
        , log_(log.clone()) {
        add_handler(collection::select, &collection_t::select);
        add_handler(collection::insert, &collection_t::insert);
        add_handler(collection::erase, &collection_t::erase);
    }


    void collection_t::insert(const py::handle& document) {
        auto is_document = py::isinstance<py::dict>(document);
        if (is_document) {
            auto doc = friedrichdb::core::make_document();
            to_document(document,*doc);
            insert_(document["_id"].cast<std::string>(), std::move(doc));
        }
    }

    auto collection_t::get(conditional_expression& cond) -> py::object {
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

    auto collection_t::search( cond) -> py::list {
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

    auto collection_t::all() -> py::list {
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
/*
    void collection_t::insert_many(py::iterable iterable) {
        auto iter = py::iter(iterable);
        for(;iter!= py::iterator::sentinel();++iter) {
            auto document = *iter;
            auto is_document = py::isinstance<py::dict>(document);
            if (is_document) {
                auto doc = friedrichdb::core::make_document();
                to_document(document,*doc);
                insert_(document["_id"].cast<std::string>(), std::move(doc));
            }
        }
    }
*/
    std::size_t collection_t::size() const {
        return size_();
    }

    void collection_t::update(py::dict fields, py::object cond) {
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

    void collection_t::remove(py::object cond) {
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

    void collection_t::drop() {
        drop_();
    }

    void collection_t::insert_(const std::string& uid, document_ptr document) {
        storage_.emplace(uid, std::move(document));
    }

    document_t* collection_t::get_(const std::string& uid) {
        auto it = storage_.find(uid);
        if (it == storage_.end()) {
            return nullptr;
        } else {
            it->second.get();
        }
    }

    std::size_t collection_t::size_() const {
        return storage_.size();
    }

    auto collection_t::begin() -> storage_t::iterator {
        return storage_.begin();
    }

    auto collection_t::end() -> storage_t::iterator {
        return storage_.end();
    }

    void collection_t::drop_() {
        storage_.clear();
    }
    auto collection_t::remove_(const std::string& key) {
        storage_.erase(key);
    }

}