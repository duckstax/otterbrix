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

    bool wrapper_collection::insert(const py::handle& document) {
        log_.trace("wrapper_collection::insert");
        auto is_document = py::isinstance<py::dict>(document);
        auto is_id = document.contains("_id") && !document["_id"].is_none();
        if (is_document && is_id) {
            components::storage::document_t doc;
            to_document(document, doc);
            auto session_tmp = duck_charmer::session_t();
            auto result =  ptr_->insert(session_tmp,name_, std::move(doc));
            log_.debug("wrapper_client::get_or_create return wrapper_database_ptr");
            return result.status;
        }
        throw std::runtime_error("wrapper_collection::insert");
    }

    wrapper_collection::~wrapper_collection() {
        log_.trace("wrapper_collection::~wrapper_collection");
    }

    wrapper_collection::wrapper_collection(const std::string& name, wrapper_dispatcher_t*ptr, log_t& log)
        : name_(name)
        , ptr_(ptr)
        , log_(log.clone()){
        log_.debug("wrapper_collection");
    }

    auto wrapper_collection::find(py::object cond) -> wrapper_cursor_ptr {
        log_.trace("wrapper_collection::find");
        wrapper_cursor_ptr ptr;
        if (py::isinstance<py::dict>(cond)) {
            components::storage::document_t condition;
            to_document(cond, condition);
            auto session_tmp = duck_charmer::session_t();
            auto result =  ptr_->find(session_tmp,name_,condition);

        }
        return ptr;
    }

    auto wrapper_collection::size() -> py::int_ {
        log_.trace("wrapper_collection::size");
        py::int_ res = 0;
        auto session_tmp = duck_charmer::session_t();
        auto result =  ptr_->size(session_tmp,name_);
        res = *result;
        return res;
    }
}