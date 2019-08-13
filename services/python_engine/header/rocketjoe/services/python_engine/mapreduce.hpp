#pragma once

#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>
#include <pybind11/pybind11.h>
#include "data_set_manager.hpp"

namespace rocketjoe { namespace services { namespace python_engine {

    using namespace pybind11::literals;
    namespace py = pybind11;
    using py_task = py::function;

            class data_set_wrapper final {
            public:
                data_set_wrapper(data_set *ds);

                auto map(py::function f) -> data_set_wrapper&;

                auto reduce_by_key(py::function f) -> data_set_wrapper&;

                auto flat_map(py::function f) -> data_set_wrapper&;

                auto collect() -> py::list;

            private:
                context*ctx_;
                data_set* ds_;
            };


            class context_wrapper final {
            public:
                context_wrapper(const std::string& name,context* ctx);

                auto text_file(const std::string& path ) -> data_set_wrapper&;

            private:
                std::string name_;
                context* context_;
                data_set_wrapper* ds_;

            };

            void add_mapreduce(py::module &pyrocketjoe, data_set_manager* dsm);


}}}