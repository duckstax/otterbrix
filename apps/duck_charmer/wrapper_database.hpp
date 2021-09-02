#pragma once
#include <iostream>
#include <memory>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <log/log.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "goblin-engineer/core.hpp"
#include "wrapper_collection.hpp"

namespace py = pybind11;

class PYBIND11_EXPORT wrapper_database final : public boost::intrusive_ref_counter<wrapper_database> {
public:
    wrapper_database(log_t&log,goblin_engineer::actor_address dispatcher_,goblin_engineer::actor_address database);
    ~wrapper_database();
    auto collection_names() -> py::list;
    wrapper_collection_ptr create(const std::string& name);
    bool drop_collection(const std::string& name);

private:
    void d_();
    bool drop_collection_;
    wrapper_collection_ptr tmp_;
    goblin_engineer::actor_address database_;
    goblin_engineer::actor_address dispatcher_;
    log_t log_;
    std::atomic_int i = 0;
    std::mutex mtx_;
    std::condition_variable cv_;
};

using wrapper_database_ptr = boost::intrusive_ptr<wrapper_database>;