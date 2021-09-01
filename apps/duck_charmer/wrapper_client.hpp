#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <log/log.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "goblin-engineer/core.hpp"
#include "wrapper_database.hpp"

namespace py = pybind11;

class PYBIND11_EXPORT wrapper_client final : public boost::intrusive_ref_counter<wrapper_client> {
public:
    wrapper_client(log_t&log,goblin_engineer::actor_address dispatcher);
    wrapper_database_ptr get_or_create(const std::string& name);
    auto database_names() -> py::list;

private:
    wrapper_database_ptr tmp_;
    void d_();
    log_t log_;
    std::atomic_int i = 0;
    goblin_engineer::actor_address dispatcher_;
    std::mutex mtx_;
    std::condition_variable cv_;
};