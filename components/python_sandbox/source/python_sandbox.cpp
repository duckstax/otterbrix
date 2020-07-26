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
        zmq::context_t*ctx,
        const components::python_sandbox_configuration& configuration,
        log_t &log,
        std::function<void(const std::string&,std::vector<std::string>)> f)
        : mode_{components::sandbox_mode_t::none}
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
        init(ctx,std::move(f));
        start();
    }

    python_interpreter::python_interpreter(const python_sandbox_configuration &configuration, log_t &log)
            : mode_{components::sandbox_mode_t::none}
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

    auto python_interpreter::jupyter_kernel_init(zmq::context_t*ctx,std::function<void(const std::string&,std::vector<std::string>)>f) -> void {
        std::ifstream connection_file{jupyter_connection_path_.string()};

        if (!connection_file) {
            throw std::logic_error("File jupyter_connection not found");
        }

        nl::json configuration;

        connection_file >> configuration;

        std::cerr << configuration.dump(4) << std::endl;

        std::string transport{configuration["transport"]};
        std::string ip{configuration["ip"]};
        auto stdin_port = std::to_string(configuration["stdin_port"].get<std::uint16_t>());
        auto stdin_address{transport + "://" + ip + ":" + stdin_port};
        stdin_socket_=std::make_unique<zmq::socket_t>(*ctx, zmq::socket_type::router);
        stdin_socket_->bind(stdin_address);

        engine_mode = false;
        jupyter_kernel = boost::intrusive_ptr<pykernel>{new pykernel{
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            engine_mode,
            boost::uuids::random_generator()(),
            detail::jupyter::make_socket_manager(std::move(f),*stdin_socket_)}};
    }

    auto python_interpreter::jupyter_engine_init(std::function<void(const std::string&,std::vector<std::string>)>f) -> void {
        std::ifstream connection_file{jupyter_connection_path_.string()};

        if (!connection_file) {
            throw std::logic_error("File jupyter_connection not found");
        }

        nl::json configuration;

        connection_file >> configuration;

        std::cerr << configuration.dump(4) << std::endl;

        auto identifier{boost::uuids::random_generator()()};

        engine_mode=true;
        jupyter_kernel = boost::intrusive_ptr<pykernel>{new pykernel{
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            engine_mode,
            identifier,
            detail::jupyter::make_socket_manager(std::move(f))}};
    }

    auto python_interpreter::start() -> void {}

    auto python_interpreter::init(zmq::context_t*ctx,std::function<void(const std::string&,std::vector<std::string>)>f) -> void {
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
            jupyter_kernel_init(ctx,std::move(f));
        } else if (components::sandbox_mode_t::jupyter_engine == mode_) {
            log_.info("jupyter engine mode");
            jupyter_engine_init(std::move(f));
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
