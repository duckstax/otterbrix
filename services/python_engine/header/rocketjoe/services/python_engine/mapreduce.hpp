#pragma once

#include <boost/filesystem.hpp>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>
#include <pybind11/pybind11.h>

namespace rocketjoe { namespace services { namespace python_engine {

    using namespace pybind11::literals;
    namespace py = pybind11;
    using py_task = py::function;

            class result_base {};

            class eager_result : public result_base {
                py::object result;

            public:
                eager_result(py::object result) : result{result} {}

                auto get() -> py::object const {
                    return result;
                }
            };

            class task {
                py_task task_handler;

            public:
                task(py_task task_handler, std::string &&name) : task_handler{task_handler} {}

                auto operator()(py::args args, py::kwargs kwargs) -> eager_result const {
                    return eager_result{task_handler(*args, **kwargs)};
                }
            };


            class data_set final /* : public py::object */ {
            public:
                auto map(py::function f) -> data_set* {
                    return this;
                }

                auto reduce_by_key(py::function f) -> data_set* {
                    return this;
                }

                auto flat_map(py::function f) -> data_set* {
                    return this;
                }

                auto collect() -> py::list {
                    return py::list{};
                }

            private:
                bool is_cached = false;
                bool is_checkpointed = false;
            };


            class context final {
            public:
                auto text_file(const std::string& name ) -> data_set* {

                }
            };

            void add_mapreduce(py::module &pyrocketjoe) {

                auto mapreduce_submodule = pyrocketjoe.def_submodule("mapreduce");
                auto celery_result_mod = mapreduce_submodule.def_submodule("result");
                auto celery_app_mod = mapreduce_submodule.def_submodule("app");

                py::class_<result_base>(celery_result_mod, "ResultBase")
                        .def(py::init<>());

                py::class_<eager_result, result_base>(celery_result_mod, "EagerResult")
                        .def(py::init<py::object>())
                        .def("get", &eager_result::get);

                py::class_<task>(celery_app_mod, "Task")
                        .def(py::init<py_task, std::string &&>())
                        .def("__call__", &task::operator());

                py::class_<context>(mapreduce_submodule, "RocketJoeContext")
                        .def(py::init<>())
                        .def("textFile",&context::text_file);

                py::class_<data_set>(mapreduce_submodule, "DataSet")
                        .def(py::init<>())
                        .def("map",&data_set::map)
                        .def("reduceByKey",&data_set::reduce_by_key)
                        .def("flatMap",&data_set::flat_map)
                        .def("collect",&data_set::collect);

            }


}}}