#pragma once
#include <memory>
#include <iostream>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "wrapper_collection.hpp"

namespace py = pybind11;

class PYBIND11_EXPORT wrapper_database final : public boost::intrusive_ref_counter<wrapper_database> {
public:
    using type_t =  friedrichdb::core::database_t;
    using pointer =type_t*;
    using unique = std::unique_ptr<type_t>;

    explicit wrapper_database(pointer ptr);
    ~wrapper_database();
    auto collection_names() -> py::list;
    wrapper_collection_ptr create(const std::string& name);
    bool drop_collection(const std::string& name);
private:
    unique ptr_;
};

using wrapper_database_ptr = boost::intrusive_ptr<wrapper_database>;