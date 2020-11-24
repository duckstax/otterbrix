#include <components/python/python.hpp>

#include <iostream>

#include <boost/uuid/uuid_io.hpp>
#include <boost/algorithm/string.hpp>
#include <pybind11/pybind11.h>
#include <nlohmann/json.hpp>

#include <detail/celery.hpp>
#include <detail/context.hpp>
#include <detail/context_manager.hpp>
#include <detail/data_set.hpp>
#include <detail/file_manager.hpp>
#include <detail/file_system.hpp>

using json = nlohmann::json;

namespace components {

    constexpr static char init_script[] = R"__(
        import sys, os
        from importlib import import_module

        class Worker(pyrocketjoe.celery.apps.Worker):
            def __init__(self, app) -> None:
                super().__init__(app)

        pyrocketjoe.celery.apps.Worker = Worker

        sys.modules['pyrocketjoe'] = pyrocketjoe
    )__";

    constexpr static char load_script[] = R"__(
        import sys, os
        from importlib import import_module

        sys.path.insert(0, os.path.dirname(path))
        module_name, _ = os.path.splitext(path)
        import_module(os.path.basename(module_name))
    )__";

    namespace nl = nlohmann;
    using namespace py::literals;

    python_t::python_t(
        const components::python_sandbox_configuration& configuration,
        log_t& log)
        : python_{}
        , pyrocketjoe_{"pyrocketjoe"}
        , file_manager_{std::make_unique<python::detail::file_manager>()}
        , context_manager_{std::make_unique<python::detail::context_manager>(*file_manager_)}{
        log_ = log.clone();
        log_.info("processing env python start ");
        log_.info(fmt::format("Mode : {0}", configuration.mode_));
        script_path_ = configuration.script_path_;
        if (!configuration.jupyter_connection_path_.empty()) {
            log_.info(fmt::format("jupyter connection path : {0}", configuration.jupyter_connection_path_.string()));
        }

        if (mode_ == sandbox_mode_t::script) {
            init();
        }

        log_.info("processing env python finish ");
    }

    auto python_t::init() -> void {
        python::detail::add_file_system(pyrocketjoe_, file_manager_.get());
        ///python::detail::add_mapreduce(pyrocketjoe, context_manager_.get());

        python::detail::add_celery(pyrocketjoe_);
        log_.info("auto python_t::init() -> void {");
        py::exec(init_script, py::globals(), py::dict("pyrocketjoe"_a = pyrocketjoe_));
    }

    python_t::~python_t() = default;

    void python_t::run_script(const std::vector<std::string>& args) {
        /// TODO: alternative PySys_SetArgv https://stackoverflow.com/questions/18245140/how-do-you-use-the-python3-c-api-for-a-command-line-driven-app
        py::list tmp;

        auto it = args.begin();
        auto end = args.end();

        it = std::next(it);

        for (; it != end; ++it) {
            tmp.append(*it);
        }

        py::module::import("sys").add_object("argv", tmp);
        py::exec(load_script, py::globals(), py::dict("path"_a = script_path_.string()));
    }

    void python_t::call_task(const std::string& task, const std::string& body) {
        std::vector<std::string> parts;
        boost::algorithm::split(parts, task, boost::algorithm::is_any_of("."));
        if (parts.size() != 2) {
            throw std::runtime_error("Task ID is invalid");
        }

        auto jbody = json::parse(body);
        if (!jbody.is_array()) {
            throw std::runtime_error("Task body is not JSON array");
        }
        auto jargs = jbody[0];
        if (!jargs.is_array()) {
            throw std::runtime_error("Task body 1st item is not JSON array");
        }
        py::list args;
        for (auto& jarg : jargs) {
            if (jarg.is_number()) {
                args.append(jarg.get<int>());
            } else if (jarg.is_string()) {
                args.append(jarg.get<std::string>());
            }
        }

        py::module::import(parts[0].c_str()).attr(parts[1].c_str())(*args);
    }

} // namespace components
