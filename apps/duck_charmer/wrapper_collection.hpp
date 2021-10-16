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

#include "wrapper_document.hpp"
#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <storage/result_insert_one.hpp>
#include <storage/forward.hpp>

namespace py = pybind11;

class PYBIND11_EXPORT wrapper_collection final : public boost::intrusive_ref_counter<wrapper_collection> {
public:
    wrapper_collection(log_t& log,goblin_engineer::actor_address dispatcher,goblin_engineer::actor_address database,goblin_engineer::actor_address collection);
    ~wrapper_collection();
    //not  using  base api  for example or test
    void insert_many(py::iterable);
    bool insert(const py::handle& document);
    auto get(py::object cond) -> py::object;
    auto search(py::object cond) -> py::list;
    auto find(py::object cond) -> py::list;
    auto all() -> py::list;
    auto size() -> py::int_;
    void update(py::dict fields, py::object cond);
    void remove(py::object cond);
    void drop();

private:
    void d_();
    services::storage::session_t session_;
    result_insert_one insert_result_ ;
    goblin_engineer::actor_address dispatcher_;
    goblin_engineer::actor_address database_;
    goblin_engineer::actor_address collection_;
    mutable log_t log_;
    std::atomic_int i = 0;
    std::mutex mtx_;
    std::condition_variable cv_;
};

using wrapper_collection_ptr = boost::intrusive_ptr<wrapper_collection>;
