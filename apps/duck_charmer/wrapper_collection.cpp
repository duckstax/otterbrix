#include "wrapper_collection.hpp"

#include "convert.hpp"
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <storage/result.hpp>
#include <storage/result_insert_one.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

bool wrapper_collection::insert(const py::handle& document) {
    log_.trace("wrapper_collection::insert");
    auto is_document = py::isinstance<py::dict>(document);
    auto is_id = document.contains("_id") && !document["_id"].is_none();
    if (is_document && is_id) {
        i = 0;
        components::storage::document_t doc;
        to_document(document, doc);
        goblin_engineer::send(
            dispatcher_,
            goblin_engineer::actor_address(),
            "insert",
            duck_charmer::session_t(),
            std::string(collection_->type().data(),collection_->type().size()),
            std::move(doc),
            std::function<void(result_insert_one&)>([this](result_insert_one& result) {
                insert_result_  = std::move(result);
                d_();
            }));
        log_.debug("wrapper_collection::insert send -> dispatcher: {}",dispatcher_->type());
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait(lk, [this]() { return i == 1; });
        log_.debug("wrapper_client::get_or_create return wrapper_database_ptr");
    }
    return insert_result_.status;
}

wrapper_collection::~wrapper_collection() {
    /// ptr_.release();
}

wrapper_collection::wrapper_collection(log_t& log, goblin_engineer::actor_address dispatcher, goblin_engineer::actor_address database, goblin_engineer::actor_address collection)
    : log_(log.clone())
    , dispatcher_(dispatcher)
    , database_(database)
    , collection_(collection) {
    log_.debug("wrapper_collection");
}

auto wrapper_collection::find(py::object cond) -> wrapper_cursor_ptr {
    log_.trace("wrapper_collection::find");
    wrapper_cursor_ptr ptr;
    if (py::isinstance<py::dict>(cond)) {
        i = 0;
        components::storage::document_t condition;
        to_document(cond, condition);
        goblin_engineer::send(
            dispatcher_,
            goblin_engineer::actor_address(),
            "find",
            duck_charmer::session_t(),
            std::string(collection_->type().data(), collection_->type().size()),
            std::move(condition),
            std::function<void(duck_charmer::session_t&,components::cursor::cursor_t*)>([&](duck_charmer::session_t& session, components::cursor::cursor_t* result) {
                ptr.reset(new  wrapper_cursor(session,result));
                d_();
            }));
        log_.debug("wrapper_collection::find send -> dispatcher: {}", dispatcher_->type());
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait(lk, [this]() { return i == 1; });
        log_.debug("wrapper_client::dispatcher return result of find");
    }
    return ptr;
}

auto wrapper_collection::all() -> py::list {
    log_.trace("wrapper_collection::all");
    /*
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
     */
}

void wrapper_collection::insert_many(py::iterable iterable) {
    log_.trace("wrapper_collection::insert_many");
    /*
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
     */
}

auto wrapper_collection::size() -> py::int_ {
    log_.trace("wrapper_collection::size");
    py::int_ res = 0;
    i = 0;
    goblin_engineer::send(
                dispatcher_,
                goblin_engineer::actor_address(),
                "size",
                duck_charmer::session_t(),
                std::string(collection_->type().data(), collection_->type().size()),
                std::function<void(result_size&)>([&](result_size &size) {
                    res = *size;
                    d_();
                }));
    log_.debug("wrapper_collection::size send -> dispatcher: {}", dispatcher_->type());
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [this]() { return i == 1; });
    log_.debug("wrapper_client::dispatcher return result of size");
    return res;
}

void wrapper_collection::update(py::dict fields, py::object cond) {
    log_.trace("wrapper_collection::update");
    /*
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
*/
}

void wrapper_collection::remove(py::object cond) {
    log_.trace("wrapper_collection::remove");
    /*
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
     */
}

void wrapper_collection::drop() {
    log_.trace("wrapper_collection::drop");
    /// cache_.clear();
    //    ptr_->drop();
}

void wrapper_collection::d_() {
    cv_.notify_all();
    i = 1;
}
