#pragma once

#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "wrapper_cursor.hpp"
#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include "forward.hpp"
#include "wrapper_dispatcher.hpp"
#include "wrapper_result.hpp"

namespace py = pybind11;
namespace duck_charmer {
    class PYBIND11_EXPORT wrapper_collection final : public boost::intrusive_ref_counter<wrapper_collection> {
    public:
        wrapper_collection(const std::string& name, const std::string &database, wrapper_dispatcher_t*, log_t& log);
        ~wrapper_collection();
        std::string print();
        std::size_t size();
        py::list insert(const py::handle& documents);
        std::string insert_one(const py::handle& document);
        py::list insert_many(const py::handle& documents);
        void update_one(py::object cond, py::dict fields, bool upsert = false);
        void update_many(py::object cond, py::dict fields, bool upsert = false);
        auto find(py::object cond) -> wrapper_cursor_ptr;
        auto find_one(py::object cond) -> py::dict;
        wrapper_result_delete delete_one(py::object cond);
        wrapper_result_delete delete_many(py::object cond);
        bool drop();

    private:
        const std::string name_;
        const std::string database_;
        wrapper_dispatcher_t* ptr_;
        mutable log_t log_;
    };

}
