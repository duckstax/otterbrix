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
#include <services/storage/result_insert_one.hpp>
#include "forward.hpp"
#include "wrapper_dispatcher.hpp"

namespace py = pybind11;
namespace duck_charmer {
    class PYBIND11_EXPORT wrapper_collection final : public boost::intrusive_ref_counter<wrapper_collection> {
    public:
        wrapper_collection(const std::string& name, wrapper_dispatcher_t*, log_t& log);
        ~wrapper_collection();
        //not  using  base api  for example or test
        bool insert(const py::handle& document);
        auto find(py::object cond) -> wrapper_cursor_ptr;
        auto size() -> py::int_;

    private:
        const std::string name_;
        wrapper_dispatcher_t* ptr_;
        mutable log_t log_;
    };

}
