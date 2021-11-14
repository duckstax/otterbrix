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
namespace duck_charmer {

wrapper_collection::wrapper_collection(const std::string& name, wrapper_dispatcher_t*ptr, log_t& log)
    : name_(name)
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
    return *(ptr_->size(session_tmp, name_));
}

bool wrapper_collection::insert(const py::handle& document) {
    log_.trace("wrapper_collection::insert");
    if (py::isinstance<py::dict>(document)) {
        components::document::document_t doc;
        to_document(document, doc);
        auto session_tmp = duck_charmer::session_t();
        auto result =  ptr_->insert(session_tmp, name_, std::move(doc));
        log_.debug("wrapper_collection::insert {}", result.status);
        return result.status;
    }
    throw std::runtime_error("wrapper_collection::insert");
    return false;
}

bool wrapper_collection::insert_one(const py::handle &document) {
}

bool wrapper_collection::insert_many(const py::handle &documents) {
}

void wrapper_collection::update(py::dict fields, py::object cond) {
}

void wrapper_collection::update_one(py::dict fields, py::object cond) {
}

auto wrapper_collection::find(py::object cond) -> wrapper_cursor_ptr {
}

auto wrapper_collection::find_one(py::object cond) -> py::dict {
}

void wrapper_collection::remove(py::object cond) {
}

void wrapper_collection::delete_one(pybind11::object cond) {
}

void wrapper_collection::delete_many(pybind11::object cond) {
}

void wrapper_collection::drop() {
}

}