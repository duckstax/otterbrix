#pragma once

#include <memory>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <goblin-engineer/core.hpp>

#include <log/log.hpp>

#include "wrapper_collection.hpp"
#include "wrapper_dispatcher.hpp"

namespace py = pybind11;
namespace duck_charmer {
    class PYBIND11_EXPORT wrapper_database final : public boost::intrusive_ref_counter<wrapper_database> {
    public:
        wrapper_database(const std::string& name, wrapper_dispatcher_t*, log_t& log);
        ~wrapper_database();
        auto collection_names() -> py::list;
        wrapper_collection_ptr create(const std::string& name);
        bool drop_collection(const std::string& name);

    private:
        const std::string name_;
        wrapper_dispatcher_t* ptr_;
        log_t log_;
        std::unordered_set<std::string> names_;
    };
}