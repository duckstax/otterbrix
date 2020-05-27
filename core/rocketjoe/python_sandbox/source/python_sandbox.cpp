#include <rocketjoe/python_sandbox/python_sandbox.hpp>

#include <cstdlib>
#include <exception>
#include <iostream>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <pybind11/pybind11.h>

#include <goblin-engineer/abstract_service.hpp>

#include <rocketjoe/python_sandbox/detail/celery.hpp>
#include <rocketjoe/python_sandbox/detail/context.hpp>
#include <rocketjoe/python_sandbox/detail/context_manager.hpp>
#include <rocketjoe/python_sandbox/detail/data_set.hpp>
#include <rocketjoe/python_sandbox/detail/file_manager.hpp>
#include <rocketjoe/python_sandbox/detail/file_system.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter.hpp>

namespace rocketjoe { namespace services {

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

    using detail::jupyter::poll_flags;

    python_sandbox_t::python_sandbox_t(goblin_engineer::components::root_manager* env, const python_sandbox_configuration& configuration,log_t&log)
        : goblin_engineer::abstract_manager_service(env, "python_sandbox")
        , mode_{sandbox_mode_t::none}
        , python_{}
        , pyrocketjoe{"pyrocketjoe"}
        , file_manager_{std::make_unique<python_sandbox::detail::file_manager>()}
        , context_manager_{std::make_unique<python_sandbox::detail::context_manager>(*file_manager_)}
        , zmq_context{nullptr}
        , jupyter_kernel_commands_polls{}
        , jupyter_kernel_infos_polls{}
        , jupyter_kernel{nullptr}
        , commands_exuctor{nullptr}
        , infos_exuctor{nullptr} {
        log_ = log.clone();
        log_.info("processing env python start ");
        log_.info(fmt::format("Mode : {0}", configuration.mode_));
        mode_ = configuration.mode_;
        script_path_ = configuration.script_path_;
        log_.info(fmt::format("jupyter connection path : {0}", configuration.jupyter_connection_path_.string()));
        jupyter_connection_path_ = configuration.jupyter_connection_path_;
        log_.info("processing env python finish ");
    }

    auto python_sandbox_t::jupyter_kernel_init() -> void {
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

        zmq_context = std::make_unique<zmq::context_t>();
        zmq::socket_t shell_socket{*zmq_context, zmq::socket_type::router};
        zmq::socket_t control_socket{*zmq_context, zmq::socket_type::router};
        zmq::socket_t stdin_socket{*zmq_context, zmq::socket_type::router};
        zmq::socket_t iopub_socket{*zmq_context, zmq::socket_type::pub};
        zmq::socket_t heartbeat_socket{*zmq_context, zmq::socket_type::rep};

        shell_socket.setsockopt(ZMQ_LINGER, 1000);
        control_socket.setsockopt(ZMQ_LINGER, 1000);
        stdin_socket.setsockopt(ZMQ_LINGER, 1000);
        iopub_socket.setsockopt(ZMQ_LINGER, 1000);
        heartbeat_socket.setsockopt(ZMQ_LINGER, 1000);

        shell_socket.bind(shell_address);
        control_socket.bind(control_address);
        stdin_socket.bind(stdin_address);
        iopub_socket.bind(iopub_address);
        heartbeat_socket.bind(heartbeat_address);

        jupyter_kernel_commands_polls = {{shell_socket, 0, ZMQ_POLLIN, 0},
                                         {control_socket, 0, ZMQ_POLLIN, 0}};
        jupyter_kernel_infos_polls = {{heartbeat_socket, 0, ZMQ_POLLIN, 0}};
        jupyter_kernel = boost::intrusive_ptr<pykernel>{new pykernel{
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            std::move(shell_socket),
            std::move(control_socket),
            std::move(stdin_socket),
            std::move(iopub_socket),
            std::move(heartbeat_socket),
            {},
            false,
            boost::uuids::random_generator()()}};
    }

    auto python_sandbox_t::jupyter_engine_init() -> void {
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

        zmq_context = std::make_unique<zmq::context_t>();
        zmq::socket_t shell_socket{*zmq_context, zmq::socket_type::router};
        zmq::socket_t control_socket{*zmq_context, zmq::socket_type::router};
        zmq::socket_t iopub_socket{*zmq_context, zmq::socket_type::pub};

        heartbeat_ping_socket = zmq::socket_t{*zmq_context,
                                              zmq::socket_type::sub};
        heartbeat_pong_socket = zmq::socket_t{*zmq_context,
                                              zmq::socket_type::dealer};

        zmq::socket_t registration_socket{*zmq_context,
                                          zmq::socket_type::dealer};

        auto identifier{boost::uuids::random_generator()()};
        auto identifier_raw{boost::uuids::to_string(identifier)};

        shell_socket.setsockopt(ZMQ_ROUTING_ID, identifier_raw.c_str(),
                                identifier_raw.size());
        control_socket.setsockopt(ZMQ_ROUTING_ID, identifier_raw.c_str(),
                                  identifier_raw.size());
        iopub_socket.setsockopt(ZMQ_ROUTING_ID, identifier_raw.c_str(),
                                identifier_raw.size());
        heartbeat_ping_socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);
        heartbeat_pong_socket.setsockopt(ZMQ_ROUTING_ID,
                                         identifier_raw.c_str(),
                                         identifier_raw.size());

        shell_socket.connect(mux_address);
        shell_socket.connect(task_address);
        control_socket.connect(control_address);
        iopub_socket.connect(iopub_address);
        heartbeat_ping_socket.connect(heartbeat_ping_address);
        heartbeat_pong_socket.connect(heartbeat_pong_address);
        registration_socket.connect(registration_address);

        jupyter_kernel_commands_polls = {{shell_socket, 0, ZMQ_POLLIN, 0},
                                         {control_socket, 0, ZMQ_POLLIN, 0}};
        jupyter_kernel = boost::intrusive_ptr<pykernel>{new pykernel{
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            std::move(shell_socket),
            std::move(control_socket),
            {},
            std::move(iopub_socket),
            {},
            std::move(registration_socket),
            true,
            std::move(identifier)}};
    }

    auto python_sandbox_t::start() -> void {
        if (mode_ == sandbox_mode_t::script) {
            commands_exuctor = std::make_unique<std::thread>([this]() {
                py::exec(load_script, py::globals(), py::dict("path"_a = script_path_.string()));
            });
        } else if (mode_ == sandbox_mode_t::jupyter_kernel ||
                   mode_ == sandbox_mode_t::jupyter_engine) {
            commands_exuctor = std::make_unique<std::thread>([this]() {
                if (mode_ == sandbox_mode_t::jupyter_engine) {
                    while (!jupyter_kernel->registration()) {
                    }
                }

                while (true) {
                    if (zmq::poll(jupyter_kernel_commands_polls) == -1) {
                        continue;
                    }

                    poll_flags polls{poll_flags::none};

                    if (jupyter_kernel_commands_polls[0].revents & ZMQ_POLLIN) {
                        polls |= poll_flags::shell_socket;
                    }

                    if (jupyter_kernel_commands_polls[1].revents & ZMQ_POLLIN) {
                        polls |= poll_flags::control_socket;
                    }

                    if (!jupyter_kernel->poll(polls)) {
                        std::exit(EXIT_SUCCESS);
                    }
                }
            });
            infos_exuctor = std::make_unique<std::thread>([this]() {
                if (mode_ == sandbox_mode_t::jupyter_kernel) {
                    while (true) {
                        if (zmq::poll(jupyter_kernel_infos_polls) == -1) {
                            continue;
                        }

                        poll_flags polls{poll_flags::none};

                        if (jupyter_kernel_infos_polls[0].revents & ZMQ_POLLIN) {
                            polls |= poll_flags::heartbeat_socket;
                        }

                        if (!jupyter_kernel->poll(polls)) {
                            break;
                        }
                    }
                } else {
                    zmq::proxy(heartbeat_ping_socket, heartbeat_pong_socket);
                }
            });
        }
    }

    void python_sandbox_t::enqueue(goblin_engineer::message, actor_zeta::executor::execution_device*) {}

    auto python_sandbox_t::init() -> void {
        python_sandbox::detail::add_file_system(pyrocketjoe, file_manager_.get());

        ///python_sandbox::detail::add_mapreduce(pyrocketjoe, context_manager_.get());

        python_sandbox::detail::add_celery(pyrocketjoe);

        if (mode_ == sandbox_mode_t::jupyter_kernel ||
            mode_ == sandbox_mode_t::jupyter_engine) {
            python_sandbox::detail::add_jupyter(pyrocketjoe, context_manager_.get());
        }

        py::exec(init_script, py::globals(), py::dict("pyrocketjoe"_a = pyrocketjoe));

        if (sandbox_mode_t::jupyter_kernel == mode_) {
            log_.info( "jupyter kernel mode");
            jupyter_kernel_init();
        } else if (sandbox_mode_t::jupyter_engine == mode_) {
            log_.info( "jupyter engine mode");
            jupyter_engine_init();
        } else {
            log_.info( "init script mode ");
        }
    }

}} // namespace rocketjoe::services
