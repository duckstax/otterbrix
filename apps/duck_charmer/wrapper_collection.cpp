#include "wrapper_collection.hpp"

#include "convert.hpp"
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <storage/result.hpp>
#include <wrapper_database.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)
namespace duck_charmer {

wrapper_collection::wrapper_collection(const std::string& name, const std::string &database, wrapper_dispatcher_t*ptr, log_t& log)
    : name_(name)
    , database_(database)
    , ptr_(ptr)
    , log_(log.clone()){
    log_.debug("wrapper_collection");
}

wrapper_collection::~wrapper_collection() {
    log_.trace("wrapper_collection::~wrapper_collection");
}

std::string wrapper_collection::print() {
    return name_;
}

std::size_t wrapper_collection::size() {
    log_.trace("wrapper_collection::size");
    auto session_tmp = duck_charmer::session_t();
    return *(ptr_->size(session_tmp, database_, name_));
}

pybind11::list wrapper_collection::insert(const py::handle& documents) {
    if (py::isinstance<py::dict>(documents)) {
        py::list result;
        auto id = insert_one(documents);
        if (!id.empty()) result.append(id);
        return result;
    }
    if (py::isinstance<py::list>(documents)) return insert_many(documents);
    return py::list();
}

std::string wrapper_collection::insert_one(const py::handle &document) {
    log_.trace("wrapper_collection::insert_one");
    if (py::isinstance<py::dict>(document)) {
        components::document::document_t doc;
        to_document(document, doc);
        auto session_tmp = duck_charmer::session_t();
        auto result = ptr_->insert_one(session_tmp, database_, name_, doc);
        log_.debug("wrapper_collection::insert_one {} inserted", result.inserted_id().size());
        return result.inserted_id();
    }
    throw std::runtime_error("wrapper_collection::insert_one");
    return std::string();
}

pybind11::list wrapper_collection::insert_many(const py::handle &documents) {
    log_.trace("wrapper_collection::insert_many");
    if (py::isinstance<py::list>(documents)) {
        std::list<components::document::document_t> docs;
        for (const auto document : documents) {
            components::document::document_t doc;
            to_document(document, doc);
            docs.push_back(std::move(doc));
        }
        auto session_tmp = duck_charmer::session_t();
        auto result = ptr_->insert_many(session_tmp, database_, name_, docs);
        log_.debug("wrapper_collection::insert_many {} inserted", result.inserted_ids().size());
        py::list list;
        for (const auto &id : result.inserted_ids()) list.append(id);
        return list;
    }
    throw std::runtime_error("wrapper_collection::insert_many");
    return py::list();
}

void wrapper_collection::update(py::dict fields, py::object cond) {
}

void wrapper_collection::update_one(py::dict fields, py::object cond) {
}

auto wrapper_collection::find(py::object cond) -> wrapper_cursor_ptr {
    log_.trace("wrapper_collection::find");
    if (py::isinstance<py::dict>(cond)) {
        components::document::document_t condition;
        to_document(cond, condition);
        auto session_tmp = duck_charmer::session_t();
        auto result = ptr_->find(session_tmp, database_, name_, std::move(condition));
        log_.debug("wrapper_collection::find {} records", result->size());
        return result;
    }
    throw std::runtime_error("wrapper_collection::find");
    return wrapper_cursor_ptr();
}

auto wrapper_collection::find_one(py::object cond) -> py::dict {
    log_.trace("wrapper_collection::find_one");
    if (py::isinstance<py::dict>(cond)) {
        components::document::document_t condition;
        to_document(cond, condition);
        auto session_tmp = duck_charmer::session_t();
        auto result = ptr_->find_one(session_tmp, database_, name_, std::move(condition));
        log_.debug("wrapper_collection::find_one {}", result.is_find());
        return from_document(*result);
    }
    throw std::runtime_error("wrapper_collection::find_one");
    return py::dict();
}

wrapper_result_delete wrapper_collection::delete_one(py::object cond) {
    log_.trace("wrapper_collection::delete_one");
    if (py::isinstance<py::dict>(cond)) {
        components::document::document_t condition;
        to_document(cond, condition);
        auto session_tmp = duck_charmer::session_t();
        auto result = ptr_->delete_one(session_tmp, database_, name_, std::move(condition));
        log_.debug("wrapper_collection::delete_one {} deleted", result.deleted_ids().size());
        return wrapper_result_delete(result);
    }
    return wrapper_result_delete();
}

wrapper_result_delete wrapper_collection::delete_many(py::object cond) {
    log_.trace("wrapper_collection::delete_many");
    if (py::isinstance<py::dict>(cond)) {
        components::document::document_t condition;
        to_document(cond, condition);
        auto session_tmp = duck_charmer::session_t();
        auto result = ptr_->delete_many(session_tmp, database_, name_, std::move(condition));
        log_.debug("wrapper_collection::delete_many {} deleted", result.deleted_ids().size());
        return wrapper_result_delete(result);
    }
    return wrapper_result_delete();
}

bool wrapper_collection::drop() {
    log_.debug("wrapper_collection::drop: {}", name_);
    result_drop_collection result;
    auto session_tmp = duck_charmer::session_t();
    result = ptr_->drop_collection(session_tmp, database_, name_);
    log_.debug("wrapper_collection::drop {}", result.is_success());
    return result.is_success();
}

}