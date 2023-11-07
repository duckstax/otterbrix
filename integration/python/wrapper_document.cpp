#include "wrapper_document.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "convert.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)
namespace ottergon {
    wrapper_document::wrapper_document(wrapper_document::pointer ptr)
        : ptr_(ptr) {}

    wrapper_document::~wrapper_document() {
        ptr_.release();
    }

    std::string wrapper_document::print() {
        return ptr_->to_json();
    }

    py::object wrapper_document::get(const std::string& key) {
        return from_object(*ptr_, key);
    }
}