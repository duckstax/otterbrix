#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_set>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <log/log.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "../../cpp/wrapper_dispatcher.hpp"
#include "forward.hpp"
#include "wrapper_cursor.hpp"

namespace py = pybind11;
namespace otterbrix {
    class PYBIND11_EXPORT wrapper_client final : public boost::intrusive_ref_counter<wrapper_client> {
    public:
        wrapper_client(log_t& log, wrapper_dispatcher_t* dispatcher);
        ~wrapper_client();
        wrapper_database_ptr get_or_create(const std::string& name);
        auto database_names() -> py::list;
        auto execute(const std::string& query) -> wrapper_cursor_ptr;

    private:
        std::unordered_map<std::string, wrapper_database_ptr> names_;
        const std::string name_;
        wrapper_dispatcher_t* ptr_;
        mutable log_t log_;
    };
} // namespace otterbrix
