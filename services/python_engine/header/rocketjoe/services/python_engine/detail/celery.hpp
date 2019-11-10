#pragma once

#include <boost/dll.hpp>

#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

            using namespace pybind11::literals;
            namespace py = pybind11;
            using py_task = py::function;

            class BOOST_SYMBOL_VISIBLE result_base {};

            class BOOST_SYMBOL_VISIBLE eager_result : public result_base {
            public:
                eager_result(py::object result);

                auto get() -> py::object const;

            private:
                py::object result;
            };

            class BOOST_SYMBOL_VISIBLE task {
            public:

                task(py_task task_handler, std::string &&name);

                auto operator()(py::args args, py::kwargs kwargs) -> eager_result const;

            private:
                py_task task_handler;
            };

            class celery {
            public:
                auto create_task() -> std::function<task(py_task)> const;
            };

            void add_celery(py::module &pyrocketjoe);

}}}}