#include "wrapper_collection.hpp"

#include "convert.hpp"
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <services/collection/result.hpp>
#include "wrapper_database.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)
namespace duck_charmer {

using components::document::document_id_t;

void generate_document_id_if_not_exists(components::document::document_ptr &document) {
    if (!document_view_t(document).is_exists(std::string_view ("_id"))) {
        document->set("_id", document_id_t().to_string());
    }
}


wrapper_collection::wrapper_collection(const std::string& name, const std::string &database, wrapper_dispatcher_t*ptr, log_t& log)
    : name_(name)
    , database_(database)
    , ptr_(ptr)
    , log_(log.clone()){
    debug(log_,"wrapper_collection");
}

wrapper_collection::~wrapper_collection() {
    trace(log_,"wrapper_collection::~wrapper_collection");
}

std::string wrapper_collection::print() {
    return name_;
}

std::size_t wrapper_collection::size() {
    trace(log_,"wrapper_collection::size");
    auto session_tmp = duck_charmer::session_id_t();
    return *(ptr_->size(session_tmp, database_, name_));
}

pybind11::list wrapper_collection::insert(const py::handle& documents) {
    if (py::isinstance<py::dict>(documents)) {
        py::list result;
        auto id = insert_one(documents);
        if (!id.empty()) {
            result.append(id);
        }
        return result;
    }
    if (py::isinstance<py::list>(documents)) {
        return insert_many(documents);
    }
    return py::list();
}

std::string wrapper_collection::insert_one(const py::handle &document) {
    trace(log_,"wrapper_collection::insert_one");
    if (py::isinstance<py::dict>(document)) {
        auto doc = to_document(document);
        generate_document_id_if_not_exists(doc);
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->insert_one(session_tmp, database_, name_, doc);
        debug(log_,"wrapper_collection::insert_one {} inserted", result.inserted_id().is_null() ? 0 : 1);
        return result.inserted_id().to_string();
    }
    throw std::runtime_error("wrapper_collection::insert_one");
    return std::string();
}

pybind11::list wrapper_collection::insert_many(const py::handle &documents) {
    trace(log_,"wrapper_collection::insert_many");
    if (py::isinstance<py::list>(documents)) {
        std::pmr::vector<components::document::document_ptr> docs;
        for (const auto document : documents) {
            auto doc = to_document(document);
            generate_document_id_if_not_exists(doc);
            docs.push_back(std::move(doc));
        }
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->insert_many(session_tmp, database_, name_, docs);
        debug(log_,"wrapper_collection::insert_many {} inserted", result.inserted_ids().size());
        py::list list;
        for (const auto &id : result.inserted_ids()) {
            list.append(id.to_string());
        }
        return list;
    }
    throw std::runtime_error("wrapper_collection::insert_many");
    return py::list();
}

wrapper_result_update wrapper_collection::update_one(py::object cond, py::object fields, bool upsert) {
    trace(log_,"wrapper_collection::update_one");
    if (py::isinstance<py::dict>(cond) && py::isinstance<py::dict>(fields)) {
        auto condition = to_document(cond);
        auto update = to_document(fields);
        generate_document_id_if_not_exists(update);
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->update_one(session_tmp, database_, name_, std::move(condition), std::move(update), upsert);
        debug(log_,"wrapper_collection::update_one {} modified {} no modified upsert id {}", result.modified_ids().size(), result.nomodified_ids().size(), result.upserted_id().to_string());
        return wrapper_result_update(result);
    }
    return wrapper_result_update(result_update(ptr_->resource()));
}

wrapper_result_update wrapper_collection::update_many(py::object cond, py::object fields, bool upsert) {
    trace(log_,"wrapper_collection::update_many");
    if (py::isinstance<py::dict>(cond) && py::isinstance<py::dict>(fields)) {
        auto condition = to_document(cond);
        auto update = to_document(fields);
        generate_document_id_if_not_exists(update);
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->update_many(session_tmp, database_, name_, std::move(condition), std::move(update), upsert);
        debug(log_,"wrapper_collection::update_many {} modified {} no modified upsert id {}", result.modified_ids().size(), result.nomodified_ids().size(), result.upserted_id().to_string());
        return wrapper_result_update(result);
    }
    return wrapper_result_update(result_update(ptr_->resource()));
}

auto wrapper_collection::find(py::object cond) -> wrapper_cursor_ptr {
    trace(log_,"wrapper_collection::find");
    if (py::isinstance<py::dict>(cond)) {
        auto condition = to_document(cond);
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->find(session_tmp, database_, name_, std::move(condition));
        debug(log_,"wrapper_collection::find {} records", result->size());
        return wrapper_cursor_ptr(new wrapper_cursor(session_tmp, result));
    }
    throw std::runtime_error("wrapper_collection::find");
    return wrapper_cursor_ptr();
}

auto wrapper_collection::find_one(py::object cond) -> py::dict {
    trace(log_,"wrapper_collection::find_one");
    if (py::isinstance<py::dict>(cond)) {
        auto condition = to_document(cond);
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->find_one(session_tmp, database_, name_, std::move(condition));
        debug(log_,"wrapper_collection::find_one {}", result.is_find());
        return from_document(*result);
    }
    throw std::runtime_error("wrapper_collection::find_one");
    return py::dict();
}

wrapper_result_delete wrapper_collection::delete_one(py::object cond) {
    trace(log_,"wrapper_collection::delete_one");
    if (py::isinstance<py::dict>(cond)) {
        auto condition = to_document(cond);
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->delete_one(session_tmp, database_, name_, std::move(condition));
        debug(log_,"wrapper_collection::delete_one {} deleted", result.deleted_ids().size());
        return wrapper_result_delete(result);
    }
    return wrapper_result_delete(result_delete(ptr_->resource()));
}

wrapper_result_delete wrapper_collection::delete_many(py::object cond) {
    trace(log_,"wrapper_collection::delete_many");
    if (py::isinstance<py::dict>(cond)) {
        auto condition = to_document(cond);
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->delete_many(session_tmp, database_, name_, std::move(condition));
        debug(log_,"wrapper_collection::delete_many {} deleted", result.deleted_ids().size());
        return wrapper_result_delete(result);
    }
    return wrapper_result_delete(result_delete(ptr_->resource()));
}

bool wrapper_collection::drop() {
    debug(log_,"wrapper_collection::drop: {}", name_);
    result_drop_collection result;
    auto session_tmp = duck_charmer::session_id_t();
    result = ptr_->drop_collection(session_tmp, database_, name_);
    debug(log_,"wrapper_collection::drop {}", result.is_success());
    return result.is_success();
}

bool wrapper_collection::create_index(py::list, index_type type) {
    debug(log_, "wrapper_collection::create_index: {}", name_);
    auto session_tmp = duck_charmer::session_id_t();
    components::ql::create_index_t index(database_, name_, type);
//    for (const auto &key : keys) {
//        index.keys_.emplace(key.cast<std::string>());
//    }
    auto result = ptr_->create_index(session_tmp, index);
    debug(log_, "wrapper_collection::create_index {}", result.is_success());
    return result.is_success();
}

}
