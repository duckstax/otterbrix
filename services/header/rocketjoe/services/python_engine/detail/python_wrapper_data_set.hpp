#pragma once

#include <deque>

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <rocketjoe/services/python_engine/detail/forward.hpp>
#include <rocketjoe/services/python_engine/detail/data_set_manager.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

                using namespace pybind11::literals;
                namespace py = pybind11;

                class python_wrapper_pipelien_data_set;

                class python_wrapper_data_set  : public data_set {
                public:
                    python_wrapper_data_set(const py::object &, mapreduce_context *);

                    python_wrapper_data_set();

                    virtual ~python_wrapper_data_set() = default;

                    auto map(py::function, bool preservesPartitioning = false) -> python_wrapper_pipelien_data_set;

                    auto reduce_by_key(py::function, bool preservesPartitioning = false) -> python_wrapper_pipelien_data_set;

                    auto flat_map(py::function, bool preservesPartitioning = false) -> python_wrapper_pipelien_data_set;

                    auto collect() -> py::list;

                    auto map_partitions_with_index(py::function, bool preservesPartitioning = false) -> python_wrapper_pipelien_data_set;

                    auto map_partitions(py::function, bool preservesPartitioning = false) -> python_wrapper_pipelien_data_set;
                protected:
                    py::object collection_;
                    mapreduce_context *ctx_;
                };


                class python_wrapper_pipelien_data_set final : public python_wrapper_data_set {
                public:
                        python_wrapper_pipelien_data_set(python_wrapper_data_set* , py::function);

                private:
                    py::function f_;

                };

}}}}