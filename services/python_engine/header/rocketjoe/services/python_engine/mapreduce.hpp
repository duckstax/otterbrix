#pragma once

#include <boost/filesystem.hpp>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>
#include <pybind11/pybind11.h>
#include "data_set_manager.hpp"

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


            class data_set_wrapper final {
            public:
                data_set_wrapper(data_set *ds):ds_(ds) {}

                auto map(py::function f) -> data_set_wrapper& {
                    ds_->transform(
                            [=](const std::string& data)-> std::string{
                                auto result =  f(data);
                                auto unpack = result.cast<std::string>();
                                return unpack;
                            }
                    );
                    return *this;
                }

                auto reduce_by_key(py::function f) -> data_set_wrapper& {
                    ds_->transform(
                            [=](const std::string& data)-> std::string{
                                auto result =  f(data);
                                auto unpack = result.cast<std::string>();
                                return unpack;
                            }
                    );

                    return *this;
                }

                auto flat_map(py::function f) -> data_set_wrapper& {
                    ds_->transform(
                            [=](const std::string& data)-> std::string{
                                auto result =  f(data);
                                auto unpack = result.cast<std::string>();
                                return unpack;
                            }
                    );
                    return *this;
                }

                auto collect() -> py::list {
                    py::list tmp{};
                    ///auto range = ds_->range();
                    ///std::copy(range.first,range.second,std::back_inserter(tmp));
                    return  tmp;

                }

            private:
                context*ctx_;
                data_set* ds_;
            };


            class context_wrapper final {
            public:
                context_wrapper(const std::string& name){

                }

                auto text_file(const std::string& path ) -> data_set_wrapper& {
                    auto* ds = context_->create_data_set(name_);
                    ds_ = new data_set_wrapper(ds);
                    return *ds_;
                }

            private:
                std::string name_;
                context* context_;
                data_set_wrapper* ds_;

            };

            void add_mapreduce(py::module &pyrocketjoe) {

                auto mapreduce_submodule = pyrocketjoe.def_submodule("mapreduce");
                auto mapreduce_result_mod = mapreduce_submodule.def_submodule("result");
                auto mapreduce_app_mod = mapreduce_submodule.def_submodule("app");

                py::class_<result_base>(mapreduce_result_mod, "ResultBase")
                        .def(py::init<>());

                py::class_<eager_result, result_base>(mapreduce_result_mod, "EagerResult")
                        .def(py::init<py::object>())
                        .def("get", &eager_result::get);

                py::class_<task>(mapreduce_app_mod, "Task")
                        .def(py::init<py_task, std::string &&>())
                        .def("__call__", &task::operator());

                py::class_<context_wrapper>(mapreduce_submodule, "RocketJoeContext")
                        .def(py::init<>())
                        .def("textFile",&context_wrapper::text_file);

                py::class_<data_set_wrapper>(mapreduce_submodule, "DataSet")
                        .def(py::init<>())
                        .def("map",&data_set_wrapper::map)
                        .def("reduceByKey",&data_set_wrapper::reduce_by_key)
                        .def("flatMap",&data_set_wrapper::flat_map)
                        .def("collect",&data_set_wrapper::collect);
            }


}}}