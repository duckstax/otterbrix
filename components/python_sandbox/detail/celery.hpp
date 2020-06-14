#pragma once

#include <memory>
#include <queue>
#include <string>
#include <unordered_map>

#include <boost/dll.hpp>
#include <boost/functional/hash.hpp>
#include <boost/optional.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/uuid/uuid.hpp>

#include <pybind11/pybind11.h>

namespace components { namespace python_sandbox { namespace detail {

    using namespace pybind11::literals;
    namespace py = pybind11;

    class celery;

    class BOOST_SYMBOL_VISIBLE result_base {};

    class BOOST_SYMBOL_VISIBLE eager_result : public result_base {
    public:
        eager_result(boost::uuids::uuid id, py::object ret_val);

        auto get() -> py::object const;

    private:
        boost::uuids::uuid id;
        py::object ret_val;
    };

    class BOOST_SYMBOL_VISIBLE async_result : public result_base {
    public:
        async_result(boost::uuids::uuid id, std::string task_name, boost::intrusive_ptr<celery> app);

        ~async_result();

        auto get() -> py::object;

    private:
        boost::uuids::uuid id;
        std::string task_name;
        py::object ret_val;
        boost::intrusive_ptr<celery> app;
    };

    class BOOST_SYMBOL_VISIBLE task {
    public:
        task(std::string task_name, py::function task_handler, boost::intrusive_ptr<celery> app);

        ~task();

        auto apply(py::args args, py::kwargs kwargs) -> eager_result const;

        auto operator()(py::args args, py::kwargs kwargs) -> eager_result const;

        auto apply_async(py::args args, py::kwargs kwargs) -> async_result const;

    private:
        std::string task_name;
        py::function task_handler;
        boost::intrusive_ptr<celery> app;
    };

    struct BOOST_SYMBOL_VISIBLE task_pending final {
        std::string task_name;
        py::args args;
        py::kwargs kwargs;
        boost::uuids::uuid task_id;
        std::size_t priority;

        task_pending(std::string task_name, py::args args, py::kwargs kwargs, boost::uuids::uuid task_id, std::size_t priority);

        bool operator<(const task_pending& rhs) const;
    };

    struct BOOST_SYMBOL_VISIBLE task_answer final {
        py::object ret_val;
        std::size_t priority;

        task_answer(py::object ret_val, std::size_t priority);

        bool operator<(const task_answer& rhs) const;
    };

    class worker final : public boost::intrusive_ref_counter<worker> {
    public:
        worker(boost::intrusive_ptr<celery> app);

        auto self() -> boost::intrusive_ptr<worker> const;

        auto poll() -> void;

    private:
        boost::intrusive_ptr<celery> app;
    };

    class BOOST_SYMBOL_VISIBLE celery final : public boost::intrusive_ref_counter<celery> {
    public:
        auto create_task(py::args args, py::kwargs kwargs) -> std::function<task(py::function)> const;

        auto send_task(std::string name, py::args args, py::kwargs kwargs, boost::uuids::uuid task_id) -> void;

        auto push_task_handler(std::string task_name, py::function task_handler) -> void;

        auto top_task_handler(std::string task_name) -> boost::optional<py::function>;

        auto push_task_pending(task_pending pending) -> void;

        auto pop_top_task_pending() -> boost::optional<task_pending>;

        auto push_task_answer(boost::uuids::uuid task_id, task_answer answer) -> void;

        auto pop_top_task_answer(boost::uuids::uuid task_id) -> boost::optional<task_answer>;

    private:
        std::unordered_map<std::string, py::function> task_handlers;
        std::priority_queue<task_pending> task_pendings;
        std::unordered_map<boost::uuids::uuid, task_answer, boost::hash<boost::uuids::uuid>> task_answers;
    };

    auto add_celery(py::module& pyrocketjoe) -> void;

}}} // namespace components::python_sandbox::detail
