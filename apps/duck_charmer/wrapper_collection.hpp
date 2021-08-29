#pragma once

#include <memory>
#include <iostream>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "wrapper_document.hpp"

namespace py = pybind11;

class PYBIND11_EXPORT wrapper_collection final : public boost::intrusive_ref_counter<wrapper_collection> {
public:
    using type_t = friedrichdb::core::collection_t;
    using pointer = type_t *;
    using unique = std::unique_ptr<type_t>;

    explicit wrapper_collection(pointer ptr);
    ~wrapper_collection();

    void insert(const py::handle &document);
    void insert_many(py::iterable);
    auto get(py::object cond) -> py::object;
    auto search(py::object cond) -> py::list;
    auto all() -> py::list;
    std::size_t size() const ;
    void update(py::dict fields,py::object cond);
    void remove(py::object cond);
    void drop();
private:
    std::unordered_map<std::string,wrapper_document_ptr> cache_;
    unique ptr_;
};

using wrapper_collection_ptr = boost::intrusive_ptr<wrapper_collection>;