#include <rocketjoe/services/python_engine/celery.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

            task::task(py::function task_handler, std::string &&/*name*/) : task_handler{task_handler} {}

            auto task::operator()(py::args args, py::kwargs kwargs) -> eager_result const {
                return eager_result{task_handler(*args, **kwargs)};
            }

            auto celery::create_task() -> const std::function<task(py::function)> {
                return [](auto task_handler) {
                    auto module_name = py::cast<std::string>(*task_handler.attr("__module__"));
                    auto qualified_name = py::cast<std::string>(*task_handler.attr("__qualname__"));
                    auto name = module_name + "." + qualified_name;

                    return task{task_handler, std::move(name)};
                };
            }

            void add_celery(py::module &pyrocketjoe) {

                auto celery_mod = pyrocketjoe.def_submodule("celery");
                auto celery_result_mod = celery_mod.def_submodule("result");
                auto celery_app_mod = celery_mod.def_submodule("app");

                py::class_<result_base>(celery_result_mod, "ResultBase")
                        .def(py::init<>());

                py::class_<eager_result, result_base>(celery_result_mod, "EagerResult")
                        .def(py::init<py::object>())
                        .def("get", &eager_result::get);

                py::class_<task>(celery_app_mod, "Task")
                        .def(py::init<py_task, std::string &&>())
                        .def("__call__", &task::operator());

                py::class_<celery>(celery_mod, "Celery")
                        .def(py::init<>())
                        .def("task", &celery::create_task);

            }

            auto eager_result::get() -> const py::object {
                return result;
            }

            eager_result::eager_result(py::object result) : result{result} {}
        }}}