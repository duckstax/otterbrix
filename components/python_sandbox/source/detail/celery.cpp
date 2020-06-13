#include <detail/celery.hpp>

#include <utility>

#include <boost/uuid/random_generator.hpp>

#include <pybind11/functional.h>
#include <pybind11/stl.h>

PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>);

namespace rocketjoe { namespace services { namespace python_sandbox { namespace detail {

    constexpr std::size_t priority_zero = 0;

    eager_result::eager_result(boost::uuids::uuid id, py::object ret_val)
        : id{std::move(id)}
        , ret_val{std::move(ret_val)} {}

    auto eager_result::get() -> py::object const {
        return ret_val;
    }

    async_result::async_result(boost::uuids::uuid id, std::string task_name, boost::intrusive_ptr<celery> app)
        : id{std::move(id)}
        , task_name{std::move(task_name)}
        , app{std::move(app)}
        , ret_val{} {}

    async_result::~async_result() {
        app->pop_top_task_answer(id);
    }

    auto async_result::get() -> py::object {
        if(ret_val) {
            return ret_val;
        }

        auto task_answer = app->pop_top_task_answer(id);

        if(task_answer) {
            ret_val = std::move(task_answer->ret_val);
        }

        return ret_val;
    }

    task::task(std::string task_name, py::function task_handler, boost::intrusive_ptr<celery> app)
        : task_name{std::move(task_name)}
        , task_handler{std::move(task_handler)}
        , app{std::move(app)} {
        this->app->push_task_handler(this->task_name, this->task_handler);
    }

    task::~task() {
        //TODO: fix memory task_handler leak
    }

    auto task::apply(py::args args, py::kwargs kwargs) -> eager_result const {
        auto task_id = boost::uuids::random_generator()();

        return eager_result{std::move(task_id), task_handler(*args, **kwargs)};
    }

    auto task::operator()(py::args args, py::kwargs kwargs) -> eager_result const {
        return apply(std::move(args), std::move(kwargs));
    }

    auto task::apply_async(py::args args, py::kwargs kwargs) -> async_result const {
        auto task_id = boost::uuids::random_generator()();

        app->send_task(task_name, std::move(args), std::move(kwargs), task_id);

        return async_result{std::move(task_id), task_name, app};
    }

    task_pending::task_pending(std::string task_name, py::args args, py::kwargs kwargs, boost::uuids::uuid task_id, std::size_t priority)
        : task_name{std::move(task_name)}
        , args{std::move(args)}
        , kwargs{std::move(kwargs)}
        , task_id{std::move(task_id)}
        , priority{priority} {}

    bool task_pending::operator<(const task_pending &rhs) const {
        return priority < rhs.priority;
    }

    task_answer::task_answer(py::object ret_val, std::size_t priority)
        : ret_val{std::move(ret_val)}
        , priority{priority} {}

    bool task_answer::operator<(const task_answer &rhs) const {
        return priority < rhs.priority;
    }

    worker::worker(boost::intrusive_ptr<celery> app) : app{std::move(app)} {}

    auto worker::self() -> boost::intrusive_ptr<worker> const {
        return boost::intrusive_ptr<worker>{this};
    }

    auto worker::poll() -> void {
        auto task_pending = app->pop_top_task_pending();

        if(!task_pending) {
            throw py::stop_iteration();
        }

        auto task_handler = app->top_task_handler(std::move(task_pending->task_name));

        if(!task_handler) {
            return;
        }

        auto result = (*task_handler)(*task_pending->args, **task_pending->kwargs);

        app->push_task_answer(std::move(task_pending->task_id), {std::move(result), priority_zero});
    }

    auto celery::create_task(py::args args, py::kwargs kwargs) -> std::function<task(py::function)> const {
        boost::intrusive_ptr<celery> self{this};

        return [self{std::move(self)}](auto task_handler) {
            auto module_name = py::cast<std::string>(*task_handler.attr("__module__"));
            auto qualified_name = py::cast<std::string>(*task_handler.attr("__qualname__"));
            auto task_name = module_name + "." + qualified_name;

            return task{std::move(task_name), std::move(task_handler), std::move(self)};
        };
    }

    auto celery::send_task(std::string name, py::args args, py::kwargs kwargs, boost::uuids::uuid task_id) -> void {
        push_task_pending({std::move(name), std::move(args), std::move(kwargs), std::move(task_id), priority_zero});
    }

    auto celery::push_task_handler(std::string task_name, py::function task_handler) -> void {
        task_handlers.insert({std::move(task_name), std::move(task_handler)});
    }

    auto celery::top_task_handler(std::string task_name) -> boost::optional<py::function> {
        auto task_handler_iter = task_handlers.find(std::move(task_name));

        if(task_handler_iter == task_handlers.end()) {
            return boost::none;
        }

        return task_handler_iter->second;
    }

    auto celery::push_task_pending(task_pending pending) -> void {
        task_pendings.push(std::move(pending));
    }

    auto celery::pop_top_task_pending() -> boost::optional<task_pending> {
        if(task_pendings.empty()) {
            return boost::none;
        }

        auto task_pending = std::move(task_pendings.top());

        task_pendings.pop();

        return task_pending;
    }

    auto celery::push_task_answer(boost::uuids::uuid task_id, task_answer answer) -> void {
        task_answers.insert({std::move(task_id), std::move(answer)});
    }

    auto celery::pop_top_task_answer(boost::uuids::uuid task_id) -> boost::optional<task_answer> {
        auto task_answer_iter = task_answers.find(task_id);

        if(task_answer_iter == task_answers.end()) {
            return boost::none;
        }

        auto task_answer = std::move(task_answer_iter->second);

        task_answers.erase(std::move(task_id));

        return task_answer;
    }

    auto add_celery(py::module &pyrocketjoe) -> void {
        auto celery_mod = pyrocketjoe.def_submodule("celery");
        auto celery_result_mod = celery_mod.def_submodule("result");
        auto celery_app_mod = celery_mod.def_submodule("app");
        auto celery_apps_mod = celery_mod.def_submodule("apps");

        py::class_<result_base>(celery_result_mod, "ResultBase")
                .def(py::init<>());

        py::class_<eager_result, result_base>(celery_result_mod, "EagerResult")
                .def(py::init<boost::uuids::uuid, py::object>())
                .def("get", &eager_result::get);

        py::class_<async_result, result_base>(celery_result_mod, "AsyncResult")
                .def(py::init<boost::uuids::uuid, std::string, boost::intrusive_ptr<celery>>())
                .def("get", &async_result::get);

        py::class_<task>(celery_app_mod, "Task")
                .def("apply", &task::apply)
                .def("__call__", &task::operator())
                .def("apply_async", &task::apply_async);

        py::class_<worker, boost::intrusive_ptr<worker>>(celery_apps_mod, "Worker")
                .def(py::init<boost::intrusive_ptr<celery>>())
                .def("__await__", &worker::self)
                .def("__iter__", &worker::self)
                .def("__next__", &worker::poll);

        py::class_<celery, boost::intrusive_ptr<celery>>(celery_mod, "Celery")
                .def(py::init<>())
                .def("task", &celery::create_task);
    }

}}}}
