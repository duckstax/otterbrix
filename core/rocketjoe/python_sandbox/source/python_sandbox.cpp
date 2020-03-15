#include <rocketjoe/python_sandbox/python_sandbox.hpp>

#include <cstdlib>
#include <exception>
#include <fstream>
#include <iostream>
#include <vector>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <pybind11/pybind11.h>

#include <actor-zeta/core.hpp>
#include <goblin-engineer.hpp>

#include <rocketjoe/network/network.hpp>
#include <rocketjoe/python_sandbox/detail/context_manager.hpp>
#include <rocketjoe/python_sandbox/detail/context.hpp>
#include <rocketjoe/python_sandbox/detail/file_manager.hpp>
#include <rocketjoe/python_sandbox/detail/celery.hpp>
#include <rocketjoe/python_sandbox/detail/file_system.hpp>
#include <rocketjoe/python_sandbox/detail/data_set.hpp>
#include <rocketjoe/python_sandbox/detail/jupyter.hpp>

namespace rocketjoe { namespace services {
    namespace po = boost::program_options;
    namespace nl = nlohmann;
    using namespace py::literals;
    using detail::jupyter::poll_flags;

    python_sandbox_t::python_sandbox_t(network::server *ptr, goblin_engineer::dynamic_config &configuration)
        : abstract_service(ptr, "python_sandbox")
        , mode{sandbox_mode::none}
        , python_{}
        , pyrocketjoe{"pyrocketjoe"}
        , file_manager_{std::make_unique<python_sandbox::detail::file_manager>()}
        , context_manager_{std::make_unique<python_sandbox::detail::context_manager>(*file_manager_)}
        , zmq_context{nullptr}
        , jupyter_kernel_commands_polls{}
        , jupyter_kernel_infos_polls{}
        , jupyter_kernel{nullptr}
        , commands_exuctor{nullptr}
        , infos_exuctor{nullptr}   {

///            add_handler(
///                    "dispatcher",
///                    [](actor_zeta::actor::context &, ::rocketjoe::network::query_context &) -> void {
///                        std::cerr << "Warning" << std::endl;
///                    }
///            );

///            add_handler(
///                    "write",
///                    [](actor_zeta::actor::context &ctx) -> void {
///                        actor_zeta::send(ctx->addresses("http"), std::move(ctx.message()));
///                    }
///            );


        std::cerr << "processing env python start " << std::endl;

        auto cfg = configuration.as_object().at("args").to<std::vector<std::string>>();

        po::options_description command_line_description("Allowed options");

        command_line_description.add_options()
            ("script", po::value<boost::filesystem::path>(),
             "path to script file")
            ("jupyter_mode", "Jupyter kernel mode")
            ("jupyter_connection", po::value<boost::filesystem::path>(),
             "path to jupyter connection file");

        po::variables_map command_line;

        po::store(po::command_line_parser(cfg).options(command_line_description)
            .run(), command_line);

        if(command_line.count("script")) {
            script_path = command_line["script"].as<boost::filesystem::path>();

            if(command_line.count("jupyter_mode")) {
                throw std::logic_error("mutually exclusive command line "
                                       "options: script and jupyter_mode");
            }

            if(!script_path.empty() && script_path.extension() == ".py") {
                mode = sandbox_mode::script;
            }
        } else if(command_line.count("jupyter_mode")) {
            if(command_line.count("jupyter_connection")) {
                jupyter_connection_path = command_line["jupyter_connection"]
                    .as<boost::filesystem::path>();
            } else {
                throw std::logic_error("the jupyter_connection command line "
                                       "parameter is undefined");
            }

            mode = sandbox_mode::jupyter;
        }


        python_sandbox::detail::add_file_system(pyrocketjoe,
                                                file_manager_.get());

        ///python_sandbox::detail::add_mapreduce(pyrocketjoe, context_manager_.get());

        python_sandbox::detail::add_celery(pyrocketjoe);

        std::cerr << "processing env python finish " << std::endl;

        if(mode == sandbox_mode::jupyter) {
          python_sandbox::detail::add_jupyter(pyrocketjoe, context_manager_.get());
          jupyter_kernel_init();
        }

        start();
    }

    constexpr static char init_script[] = R"__(
        import sys, os
        from importlib import import_module

        class Worker(pyrocketjoe.celery.apps.Worker):
            def __init__(self, app) -> None:
                super().__init__(app)

        pyrocketjoe.celery.apps.Worker = Worker

        sys.modules['pyrocketjoe'] = pyrocketjoe
        sys.path.insert(0, os.path.dirname(path))

        module_name, _ = os.path.splitext(path)
        import_module(os.path.basename(module_name))
    )__";

    auto python_sandbox_t::jupyter_kernel_init() -> void {
        std::ifstream connection_file{jupyter_connection_path.string()};

        if(!connection_file) {
            throw std::logic_error("FIle jupyter_connection not found");
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

        jupyter_kernel_commands_polls = {{shell_socket,   0, ZMQ_POLLIN, 0},
                                         {control_socket, 0, ZMQ_POLLIN, 0}};
        jupyter_kernel_infos_polls = {{heartbeat_socket, 0, ZMQ_POLLIN, 0}};
        jupyter_kernel = boost::intrusive_ptr<pykernel>{new pykernel{
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            std::move(shell_socket), std::move(control_socket),
            std::move(stdin_socket), std::move(iopub_socket),
            std::move(heartbeat_socket)
        }};
    }

    auto python_sandbox_t::start() -> void {
        if(mode == sandbox_mode::script) {
            commands_exuctor = std::make_unique<std::thread>([this]() {
                py::exec(init_script, py::globals(), py::dict(
                    "path"_a = script_path.string(),
                    "pyrocketjoe"_a = pyrocketjoe
                ));
            });
        } else if(mode == sandbox_mode::jupyter) {
            commands_exuctor = std::make_unique<std::thread>([this]() {
                while(true) {
                    if(zmq::poll(jupyter_kernel_commands_polls) == -1) {
                        continue;
                    }

                    poll_flags polls{poll_flags::none};

                    if(jupyter_kernel_commands_polls[0].revents & ZMQ_POLLIN) {
                        polls |= poll_flags::shell_socket;
                    }

                    if(jupyter_kernel_commands_polls[1].revents & ZMQ_POLLIN) {
                        polls |= poll_flags::control_socket;
                    }

                    if(!jupyter_kernel->poll(polls)) {
                        std::exit(EXIT_SUCCESS);
                    }
                }
            });
            infos_exuctor = std::make_unique<std::thread>([this]() {
                while(true) {
                    if(zmq::poll(jupyter_kernel_infos_polls) == -1) {
                        continue;
                    }

                    poll_flags polls{poll_flags::none};

                    if(jupyter_kernel_infos_polls[0].revents & ZMQ_POLLIN) {
                        polls |= poll_flags::heartbeat_socket;
                    }

                    if(!jupyter_kernel->poll(polls)) {
                        break;
                    }
                }
            });
        }
    }
}}
