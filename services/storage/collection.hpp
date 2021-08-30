#pragma once
#include <memory>
#include <unordered_map>

#include "protocol/base.hpp"
#include "storage/document.hpp"
#include <goblin-engineer/core.hpp>

#include "log/log.hpp"

#include "forward.hpp"
#include "protocol/forward.hpp"
#include "route.hpp"

namespace services::storage {

    class collection_t final : public goblin_engineer::abstract_service {

        collection_t(database_ptr database, log_t& log);


        void insert(const py::handle &document) {
            auto is_document = py::isinstance<py::dict>(document);
            if (is_document) {
                auto doc = friedrichdb::core::make_document();
                to_document(document,*doc);
                insert_(document["_id"].cast<std::string>(), std::move(doc));
            }
        }


        auto get(py::object cond) -> py::object {
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


        auto search(py::object cond) -> py::list {
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

        auto all() -> py::list {
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

        void insert_many(py::iterable iterable) {
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

        std::size_t size() const {
            return size_();
        }

        void update(py::dict fields, py::object cond) {
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

        void remove(py::object cond) {
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

        void drop() {
           drop_();
        }

        using storage_t = std::unordered_map<std::string, document_ptr>;
        using iterator = typename storage_t::iterator ;

        void insert_(const std::string& uid, document_ptr document) {
            storage_.emplace(uid, std::move(document));
        }

        document_t *get_(const std::string& uid) {
            auto it = storage_.find(uid);
            if (it == storage_.end()) {
                return nullptr;
            } else {
                it->second.get();
            }
        }

        std::size_t size_() const {
            return storage_.size();
        }

        auto begin() -> iterator {
            return storage_.begin();
        }

        auto end() -> iterator {
            return storage_.end();
        }

        auto remove_(const std::string& key ){
            storage_.erase(key);
        }

        void drop_(){
            storage_.clear();
        }

    private:
        log_t log_;
        storage_t storage_;
    };

    using collection_ptr = goblin_engineer::intrusive_ptr<collection_t>;

} // namespace kv