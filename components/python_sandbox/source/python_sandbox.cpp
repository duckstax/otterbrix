#include <components/python_sandbox/python_sandbox.hpp>

#include <cstdlib>
#include <exception>
#include <iostream>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <pybind11/pybind11.h>

#include <detail/celery.hpp>
#include <detail/context.hpp>
#include <detail/context_manager.hpp>
#include <detail/data_set.hpp>
#include <detail/file_manager.hpp>
#include <detail/file_system.hpp>
#include <detail/jupyter.hpp>

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

    python_interpreter::python_interpreter(
        const components::python_sandbox_configuration& configuration,
        components::log_t& log, std::function<void(const std::string&,std::vector<std::string>)>f)
        : zmq(std::move(f))
        , mode_{components::sandbox_mode_t::none}
        , python_{}
        , pyrocketjoe{"pyrocketjoe"}
        , file_manager_{std::make_unique<python_sandbox::detail::file_manager>()}
        , context_manager_{std::make_unique<python_sandbox::detail::context_manager>(*file_manager_)}
        , jupyter_kernel{nullptr}{
        log_ = log.clone();
        log_.info("processing env python start ");
        log_.info(fmt::format("Mode : {0}", configuration.mode_));
        mode_ = configuration.mode_;
        script_path_ = configuration.script_path_;
        if (!configuration.jupyter_connection_path_.empty()) {
            log_.info(fmt::format("jupyter connection path : {0}", configuration.jupyter_connection_path_.string()));
        }
        jupyter_connection_path_ = configuration.jupyter_connection_path_;
        log_.info("processing env python finish ");
    }

    auto python_interpreter::jupyter_kernel_init() -> void {
        std::ifstream connection_file{jupyter_connection_path_.string()};

        if (!connection_file) {
            throw std::logic_error("File jupyter_connection not found");
        }

        nl::json configuration;

        connection_file >> configuration;

        std::cerr << configuration.dump(4) << std::endl;

        std::string transport{configuration["transport"]};
        std::string ip{configuration["ip"]};
        auto shell_port{std::to_string(configuration["shell_port"]
                                           .get<std::uint16_t>())};
        auto control_port{std::to_string(configuration["control_port"]
                                             .get<std::uint16_t>())};
        auto stdin_port{std::to_string(configuration["stdin_port"]
                                           .get<std::uint16_t>())};
        auto iopub_port{std::to_string(configuration["iopub_port"]
                                           .get<std::uint16_t>())};
        auto heartbeat_port{std::to_string(configuration["hb_port"]
                                               .get<std::uint16_t>())};
        auto shell_address{transport + "://" + ip + ":" + shell_port};
        auto control_address{transport + "://" + ip + ":" + control_port};
        auto stdin_address{transport + "://" + ip + ":" + stdin_port};
        auto iopub_address{transport + "://" + ip + ":" + iopub_port};
        auto heartbeat_address{transport + "://" + ip + ":" + heartbeat_port};

        engine_mode = false;
        jupyter_kernel = boost::intrusive_ptr<pykernel>{new pykernel{
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            engine_mode,
            boost::uuids::random_generator()(),
            zmq}};
    }

    auto python_interpreter::jupyter_engine_init() -> void {
        std::ifstream connection_file{jupyter_connection_path_.string()};

        if (!connection_file) {
            throw std::logic_error("File jupyter_connection not found");
        }

        nl::json configuration;

        connection_file >> configuration;

        std::cerr << configuration.dump(4) << std::endl;

        std::string interface{configuration["interface"]};
        auto mux_port{std::to_string(configuration["mux"]
                                         .get<std::uint16_t>())};
        auto task_port{std::to_string(configuration["task"]
                                          .get<std::uint16_t>())};
        auto control_port{std::to_string(configuration["control"]
                                             .get<std::uint16_t>())};
        auto iopub_port{std::to_string(configuration["iopub"]
                                           .get<std::uint16_t>())};
        auto heartbeat_ping_port{std::to_string(configuration["hb_ping"]
                                                    .get<std::uint16_t>())};
        auto heartbeat_pong_port{std::to_string(configuration["hb_pong"]
                                                    .get<std::uint16_t>())};
        auto registration_port{std::to_string(configuration["registration"]
                                                  .get<std::uint16_t>())};
        auto mux_address{interface + ":" + mux_port};
        auto task_address{interface + ":" + task_port};
        auto control_address{interface + ":" + control_port};
        auto iopub_address{interface + ":" + iopub_port};
        auto heartbeat_ping_address{interface + ":" + heartbeat_ping_port};
        auto heartbeat_pong_address{interface + ":" + heartbeat_pong_port};
        auto registration_address{interface + ":" + registration_port};

        auto identifier{boost::uuids::random_generator()()};
        auto identifier_raw{boost::uuids::to_string(identifier)};

        engine_mode=true;
        jupyter_kernel = boost::intrusive_ptr<pykernel>{new pykernel{
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            engine_mode,
            std::move(identifier),
            zmq}};
    }

    auto python_interpreter::start() -> void {}

    auto python_interpreter::init() -> void {
        python_sandbox::detail::add_file_system(pyrocketjoe, file_manager_.get());

        ///python_sandbox::detail::add_mapreduce(pyrocketjoe, context_manager_.get());

        python_sandbox::detail::add_celery(pyrocketjoe);

        if (mode_ == components::sandbox_mode_t::jupyter_kernel ||
            mode_ == components::sandbox_mode_t::jupyter_engine) {
            python_sandbox::detail::add_jupyter(pyrocketjoe, context_manager_.get());
        }

        py::exec(init_script, py::globals(), py::dict("pyrocketjoe"_a = pyrocketjoe));

        if (components::sandbox_mode_t::jupyter_kernel == mode_) {
            log_.info("jupyter kernel mode");
            jupyter_kernel_init();
        } else if (components::sandbox_mode_t::jupyter_engine == mode_) {
            log_.info("jupyter engine mode");
            jupyter_engine_init();
        } else {
            log_.info("init script mode ");
        }
    }

    python_interpreter::~python_interpreter() = default;

    void python_interpreter::run_script(const std::vector<std::string>& args) {
        /// TODO: alternative PySys_SetArgv https://stackoverflow.com/questions/18245140/how-do-you-use-the-python3-c-api-for-a-command-line-driven-app
        py::list tmp;

        auto it =  args.begin();
        auto end = args.end();

        it = std::next(it);

        for (;it!=end;++it) {
            tmp.append(*it);
        }

        py::module::import("sys").add_object("argv", tmp);
        py::exec(load_script, py::globals(), py::dict("path"_a = script_path_.string()));
    }

    auto python_interpreter::dispatch_shell(std::vector<std::string> msgs) -> void {
        jupyter_kernel->dispatch_shell(msgs);
    }

    auto python_interpreter::dispatch_control(std::vector<std::string> msgs) -> void {
        jupyter_kernel->dispatch_control(msgs);
    }

    auto python_interpreter::registration(std::vector<std::string>) -> void {

    }

} // namespace components
