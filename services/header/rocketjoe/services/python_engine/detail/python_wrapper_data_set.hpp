#pragma once

#include <deque>

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <rocketjoe/services/python_engine/detail/forward.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

                using namespace pybind11::literals;
                namespace py = pybind11;

                class python_wrapper_data_set {
                public:
                    python_wrapper_data_set(const py::object& collections,context *ctx);

                    python_wrapper_data_set();

                    virtual ~python_wrapper_data_set() = default;

                    auto map(py::function f) -> python_wrapper_data_set;

                    auto reduce_by_key(py::function f, bool preservesPartitioning=false) -> python_wrapper_data_set;

                    auto flat_map(py::function f, bool preservesPartitioning=false) -> python_wrapper_data_set;

                    auto collect() -> py::list;

                protected:
                    py::object collection_;
                    context *ctx_;
                };


                class python_wrapper_pipelien_data_set final : public python_wrapper_data_set {
                public:
                        python_wrapper_pipelien_data_set(python_wrapper_data_set* data_set, py::function f  ){}

                private:
                    py::function f_;

                };

}}}}