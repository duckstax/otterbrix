#pragma once

#include <iostream>
#include <memory>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <components/document/document.hpp>

namespace py = pybind11;

class PYBIND11_EXPORT wrapper_document final : public boost::intrusive_ref_counter<wrapper_document> {
public:
    using type_t = components::storage::document_t;
    using pointer = type_t*;
    using unique = std::unique_ptr<type_t>;

    explicit wrapper_document(pointer ptr);
    ~wrapper_document();
    auto print() -> std::string;
    auto get(const std::string& name) -> py::object;
    auto raw() -> pointer {
        return ptr_.get();
    }

private:
    unique ptr_;
};

using wrapper_document_ptr = boost::intrusive_ptr<wrapper_document>;
