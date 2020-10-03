#include "jupyter.hpp"

#include <actor-zeta/core.hpp>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <components/configuration/configuration.hpp>

#include <nlohmann/json.hpp>

#include <components/buffer/zmq_buffer.hpp>

#include <fmt/core.h>

namespace services {

    auto send(zmq::socket_ref socket, std::vector<std::string> msgs) -> void {
        auto log = components::get_logger();
        std::vector<zmq::const_buffer> msgs_for_send;

        msgs_for_send.reserve(msgs.size());

        for (const auto& msg : msgs) {
            log.info(fmt::format("Socket write: {}", msg));
            msgs_for_send.push_back(zmq::buffer(std::move(msg)));
        }

        auto result = zmq::send_multipart(socket, std::move(msgs_for_send));

        if (!result) {
            throw std::logic_error("Error sending ZeroMQ message");
        } else {
            log.info("Socket write !");
        }
    }

    namespace nl = nlohmann;

    jupyter::jupyter(
        const components::configuration& cfg,
        components::log_t& log)
        : actor_zeta::supervisor("jupyter")
        , coordinator_(new actor_zeta::executor_t<actor_zeta::work_sharing>(1, 1000))
        , mode_{components::sandbox_mode_t::none}
        , zmq_context_{nullptr}
        , jupyter_kernel_commands_polls{}
        , jupyter_kernel_infos_polls{}
        , commands_exuctor{nullptr}
        , infos_exuctor{nullptr}
        , log_(log.clone()) {
        log_.info("jupyter start construct");
        log_.info(fmt::format("Mode : {0}", cfg.python_configuration_.mode_));
        mode_ = cfg.python_configuration_.mode_;
        if (!cfg.python_configuration_.jupyter_connection_path_.empty()) {
            log_.info(fmt::format("jupyter connection path : {0}", cfg.python_configuration_.jupyter_connection_path_.string()));
        }
        jupyter_connection_path_ = cfg.python_configuration_.jupyter_connection_path_;
        add_handler("write", &jupyter::write);
        init();
        log_.info("jupyter finish construct");
    }

    auto jupyter::jupyter_kernel_init() -> void {
        std::ifstream connection_file{jupyter_connection_path_.string()};

        if (!connection_file) {
            throw std::logic_error("File jupyter_connection not found");
        }

        nl::json configuration;

        connection_file >> configuration;

        log_.info(configuration.dump(4));

        std::string transport{configuration["transport"]};
        std::string ip{configuration["ip"]};
        auto shell_port{std::to_string(configuration["shell_port"]
                                           .get<std::uint16_t>())};
        auto control_port{std::to_string(configuration["control_port"]
                                             .get<std::uint16_t>())};
        auto iopub_port{std::to_string(configuration["iopub_port"]
                                           .get<std::uint16_t>())};
        auto heartbeat_port{std::to_string(configuration["hb_port"]
                                               .get<std::uint16_t>())};
        auto shell_address{transport + "://" + ip + ":" + shell_port};
        auto control_address{transport + "://" + ip + ":" + control_port};
        auto iopub_address{transport + "://" + ip + ":" + iopub_port};
        auto heartbeat_address{transport + "://" + ip + ":" + heartbeat_port};

        zmq_context_ = std::make_unique<zmq::context_t>();
        shell_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::router);
        control_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::router);
        iopub_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::pub);
        heartbeat_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::rep);

        shell_socket->setsockopt(ZMQ_LINGER, 1000);
        control_socket->setsockopt(ZMQ_LINGER, 1000);
        iopub_socket->setsockopt(ZMQ_LINGER, 1000);
        heartbeat_socket->setsockopt(ZMQ_LINGER, 1000);

        shell_socket->bind(shell_address);
        control_socket->bind(control_address);
        iopub_socket->bind(iopub_address);
        heartbeat_socket->bind(heartbeat_address);

        jupyter_kernel_commands_polls = {{*shell_socket, 0, ZMQ_POLLIN, 0},
                                         {*control_socket, 0, ZMQ_POLLIN, 0}};
        jupyter_kernel_infos_polls = {{*heartbeat_socket, 0, ZMQ_POLLIN, 0}};
        engine_mode = false;
    }

    auto jupyter::jupyter_engine_init() -> void {
        std::ifstream connection_file{jupyter_connection_path_.string()};

        if (!connection_file) {
            throw std::logic_error("File jupyter_connection not found");
        }

        nl::json configuration;

        connection_file >> configuration;

        log_.info(configuration.dump(4));

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

        zmq_context_ = std::make_unique<zmq::context_t>();
        shell_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::router);
        control_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::router);
        iopub_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::pub);

        heartbeat_ping_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::sub);
        heartbeat_pong_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::dealer);

        registration_socket = std::make_unique<zmq::socket_t>(*zmq_context_, zmq::socket_type::dealer);

        identifier_ = boost::uuids::random_generator()();
        auto identifier_raw = boost::uuids::to_string(identifier_);
        shell_socket->setsockopt(ZMQ_ROUTING_ID, identifier_raw.c_str(),identifier_raw.size());
        control_socket->setsockopt(ZMQ_ROUTING_ID, identifier_raw.c_str(),identifier_raw.size());
        iopub_socket->setsockopt(ZMQ_ROUTING_ID, identifier_raw.c_str(),identifier_raw.size());
        heartbeat_ping_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
        heartbeat_pong_socket->setsockopt(ZMQ_ROUTING_ID,identifier_raw.c_str(),identifier_raw.size());

        shell_socket->connect(mux_address);
        shell_socket->connect(task_address);
        control_socket->connect(control_address);
        iopub_socket->connect(iopub_address);
        heartbeat_ping_socket->connect(heartbeat_ping_address);
        heartbeat_pong_socket->connect(heartbeat_pong_address);
        registration_socket->connect(registration_address);

        jupyter_kernel_commands_polls = {{*shell_socket, 0, ZMQ_POLLIN, 0},
                                         {*control_socket, 0, ZMQ_POLLIN, 0}};
        engine_mode = true;
    }

    auto jupyter::start() -> void {
        executor().start();
        for (auto& i : init_) {
            i();
        }

        if (mode_ == components::sandbox_mode_t::jupyter_kernel) {
            log_.info("jupyter kernel heartbeat");
            infos_exuctor = std::make_unique<std::thread>(
                [this]() {
                    bool e = true;
                    while (e) {
                        if (zmq::poll(jupyter_kernel_infos_polls) == -1) {
                            continue;
                        }
                        std::vector<zmq::message_t> msgs;
                        while (zmq::recv_multipart(*heartbeat_socket,
                                                   std::back_inserter(msgs),
                                                   zmq::recv_flags::dontwait)) {
                            for (const auto& msg : msgs) {
                                log_.info(fmt::format("Heartbeat: {}", msg.to_string()));
                            }

                            zmq::send_multipart(*heartbeat_socket, std::move(msgs), zmq::send_flags::dontwait);
                        }
                    }
                });
        } else if (mode_ == components::sandbox_mode_t::jupyter_engine) {
            log_.info("jupyter engine heartbeat");
            infos_exuctor = std::make_unique<std::thread>(
                [this]() {
                    zmq::proxy(*heartbeat_ping_socket, *heartbeat_pong_socket);
                });
        } else {
            log_.error("Error not heartbeat");
        }

        if (mode_ == components::sandbox_mode_t::jupyter_engine) {
            auto py = addresses("python");
            std::vector<zmq::message_t> msgs;

            if (!zmq::recv_multipart(*registration_socket, std::back_inserter(msgs))) {
            }

            std::vector<std::string> msgs_for_parse;
            msgs_for_parse.reserve(msgs.size());

            for (const auto& msg : msgs) {
                log_.info(fmt::format("Registration: {}", msg.to_string()));
                msgs_for_parse.push_back(std::move(msg.to_string()));
            }

            actor_zeta::send(py, self(), "registration", msgs_for_parse);
        }

        bool e = true;
        while (e) {
            auto py = addresses("python");
            if (zmq::poll(jupyter_kernel_commands_polls) == -1) {
                continue;
            }

            std::vector<zmq::message_t> msgs;

            actor_zeta::send(py, self(), "start_session");

            if (jupyter_kernel_commands_polls[0].revents & ZMQ_POLLIN) {
                while (zmq::recv_multipart(*shell_socket,
                                           std::back_inserter(msgs),
                                           zmq::recv_flags::dontwait)) {
                    std::vector<std::string> msgs_for_parse;

                    msgs_for_parse.reserve(msgs.size());

                    for (const auto& msg : msgs) {
                        log_.info(fmt::format("Shell: {}", msg.to_string()));
                        msgs_for_parse.push_back(std::move(msg.to_string()));
                    }

                    actor_zeta::send(py, self(), "shell", components::buffer("shell", msgs_for_parse));
                }
            }

            if (jupyter_kernel_commands_polls[1].revents & ZMQ_POLLIN) {
                while (zmq::recv_multipart(*control_socket,
                                           std::back_inserter(msgs),
                                           zmq::recv_flags::dontwait)) {
                    std::vector<std::string> msgs_for_parse;

                    msgs_for_parse.reserve(msgs.size());

                    for (const auto& msg : msgs) {
                        log_.info(fmt::format("Control: {}", msg.to_string()));
                        msgs_for_parse.push_back(std::move(msg.to_string()));
                    }

                    actor_zeta::send(py, self(), "control", components::buffer("control", msgs_for_parse));
                }
            }

            actor_zeta::send(py, self(), "stop_session");
        }
    }

    auto jupyter::init() -> void {
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

    jupyter::~jupyter() = default;

    void jupyter::enqueue(goblin_engineer::message msg, actor_zeta::executor::execution_device*) {
        auto tmp = std::move(msg);
        log_.info(fmt::format("message command: {}", std::string(tmp.command().data(), tmp.command().size())));
        log_.info(fmt::format("message sender: {}", std::string(tmp.sender()->name().data(), tmp.sender()->name().size())));
        log_.info(fmt::format("actor name: {}", std::string(this->name().data(), this->name().size())));
        set_current_message(std::move(tmp));
        dispatch().execute(*this);
    }

    auto jupyter::write(components::zmq_buffer_t& msg) -> void {
        log_.info(" auto jupyter::write");
        if ("iopub" == msg->id()) {
            log_.info(" auto jupyter::write iopub");
            return send(*iopub_socket, msg->msg());
        }

        if ("shell" == msg->id()) {
            log_.info(" auto jupyter::write shell");
            return send(*shell_socket, msg->msg());
        }

        if ("control" == msg->id()) {
            log_.info(" auto jupyter::write control");
            return send(*control_socket, msg->msg());
        }

        if ("stdion" == msg->id()) {
            if (engine_mode) {
                log_.info(" auto jupyter::write stddin");
                return send(*iopub_socket, msg->msg());
            }
        }

        if ("registration" == msg->id()) {
            log_.info(" auto jupyter::write registration");
            return send(*registration_socket, msg->msg());
        }

        log_.info("Non write");
    }

    zmq::context_t& jupyter::zmq_context() {
        return *zmq_context_;
    }

    auto jupyter::executor() noexcept -> actor_zeta::abstract_executor& {
        return *coordinator_;
    }

    auto jupyter::join(actor_zeta::actor tmp) -> actor_zeta::actor_address {
        auto actor = std::move(tmp);
        auto address = actor->address();
        actor_zeta::link(*this, address);
        actor_storage_.emplace_back(std::move(actor));
        return address;
    }

    auto jupyter::join(actor_zeta::intrusive_ptr<actor_zeta::supervisor> tmp) -> actor_zeta::actor_address {
        auto supervisor = std::move(tmp);
        auto address = supervisor->address();
        actor_zeta::link(*this, address);
        storage_.emplace_back(std::move(supervisor));
        return address;
    }

    auto jupyter::pre_hook(std::function<void()> f) -> void {
        init_.emplace_back(std::move(f));
    }

} // namespace services
