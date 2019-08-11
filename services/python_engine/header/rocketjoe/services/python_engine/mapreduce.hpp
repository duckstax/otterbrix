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

            class map_reduce final {
            public:
                auto create_task() -> std::function<task(py_task)> const {
                    return [](auto task_handler) {
                        auto module_name = py::cast<std::string>(*task_handler.attr("__module__"));
                        auto qualified_name = py::cast<std::string>(*task_handler.attr("__qualname__"));
                        auto name = module_name + "." + qualified_name;

                        return task{task_handler, std::move(name)};
                    };
                }

                auto map() -> void {

                }

                auto reduce() -> void {

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

                py::class_<map_reduce>(mapreduce_submodule, "MapReduce")
                        .def(py::init<>())
                        .def("task", &map_reduce::create_task)
                        .def("map",&map_reduce::map)
                        .def("reduce",&map_reduce::map);
            }


}}}