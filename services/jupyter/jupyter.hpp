#pragma once

#include <atomic>
#include <memory>
#include <queue>
#include <unordered_set>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <goblin-engineer/abstract_manager_service.hpp>
#include <goblin-engineer/components/root_manager.hpp>

#include <components/log/log.hpp>

#include <actor-zeta/core.hpp>
#include <boost/filesystem/path.hpp>
#include <components/configuration/configuration.hpp>

namespace services {

    class zmq_buffer_tt final : public boost::intrusive_ref_counter<zmq_buffer_tt> {
    public:
        zmq_buffer_tt()
            : id_("") {}
        zmq_buffer_tt(const zmq_buffer_tt&) = delete;
        zmq_buffer_tt& operator=(const zmq_buffer_tt&) = delete;

        zmq_buffer_tt(const std::string& fd, std::vector<std::string> msgs)
            : id_(fd)
            , msg_(std::move(msgs)) {}

        const std::string& id() const {
            return id_;
        }

        const std::vector<std::string>& msg() const {
            return msg_;
        }

    private:
        const std::string id_;
        std::vector<std::string> msg_;
    };

    using zmq_buffer_t = boost::intrusive_ptr<zmq_buffer_tt>;

    template<class... Args>
    auto buffer(Args&&... args) -> zmq_buffer_t {
        return boost::intrusive_ptr<zmq_buffer_tt>(new zmq_buffer_tt(std::forward<Args>(args)...));
    }

    using sender_t = std::function<void(zmq_buffer_t)>;

    class jupyter final : public goblin_engineer::abstract_manager_service {
    public:
        jupyter() = delete;
        jupyter(const jupyter&) = delete;
        jupyter& operator=(const jupyter&) = delete;

        jupyter(goblin_engineer::components::root_manager*, const components::python_sandbox_configuration&, components::log_t&);

        ~jupyter();

        zmq::context_t* zmq_context();

        auto start() -> void;

        auto init() -> void;

        void enqueue(goblin_engineer::message, actor_zeta::execution_device*) override;

        auto write(const std::string&, std::vector<std::string>&) -> void;

    private:
        auto jupyter_engine_init() -> void;

        auto jupyter_kernel_init() -> void;

        boost::filesystem::path jupyter_connection_path_;
        components::sandbox_mode_t mode_;
        std::unique_ptr<zmq::context_t> zmq_context_;
        std::unique_ptr<zmq::socket_t> heartbeat_ping_socket;
        std::unique_ptr<zmq::socket_t> heartbeat_pong_socket;
        std::vector<zmq::pollitem_t> jupyter_kernel_commands_polls;
        std::vector<zmq::pollitem_t> jupyter_kernel_infos_polls;
        std::unique_ptr<std::thread> commands_exuctor; ///TODO: HACK
        std::unique_ptr<std::thread> infos_exuctor;    ///TODO: HACK
        components::log_t log_;
        std::unique_ptr<zmq::socket_t> shell_socket;
        std::unique_ptr<zmq::socket_t> control_socket;
        std::unique_ptr<zmq::socket_t> stdin_socket;
        std::unique_ptr<zmq::socket_t> iopub_socket;
        std::unique_ptr<zmq::socket_t> heartbeat_socket;
        std::unique_ptr<zmq::socket_t> registration_socket;
        bool engine_mode;
    };

} // namespace services
