#pragma once

#include <memory>
#include <iostream>
#include <mutex>
#include <condition_variable>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "wrapper_document.hpp"

namespace py = pybind11;

class PYBIND11_EXPORT wrapper_collection final : public boost::intrusive_ref_counter<wrapper_collection> {
public:
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
   std::mutex mtx_;
};

using wrapper_collection_ptr = boost::intrusive_ptr<wrapper_collection>;