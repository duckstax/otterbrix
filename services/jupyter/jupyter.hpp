#pragma once

#include <atomic>
#include <memory>
#include <queue>
#include <unordered_set>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/uuid/uuid.hpp>

#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <goblin-engineer/abstract_manager_service.hpp>
#include <goblin-engineer/components/root_manager.hpp>

#include <components/log/log.hpp>

#include <actor-zeta/core.hpp>
#include <boost/filesystem/path.hpp>
#include <components/buffer/zmq_buffer.hpp>
#include <components/configuration/configuration.hpp>
#include <components/ssh_forwarder/ssh_forwarder.hpp>

namespace services {

    class jupyter final : public actor_zeta::supervisor {
    public:
        jupyter() = delete;
        jupyter(const jupyter&) = delete;
        jupyter& operator=(const jupyter&) = delete;

        jupyter(const components::configuration&, components::log_t&);

        auto executor() noexcept -> actor_zeta::abstract_executor& override;

        auto join(actor_zeta::actor) -> actor_zeta::actor_address override;

        auto join(actor_zeta::intrusive_ptr<actor_zeta::supervisor>) -> actor_zeta::actor_address;

        ~jupyter() override;

        zmq::context_t& zmq_context();

        auto start() -> void;

        void enqueue(goblin_engineer::message, actor_zeta::execution_device*) override;

        auto pre_hook(std::function<void()> f) -> void;

        auto identifier(boost::uuids::uuid identifier) -> void;

        auto identifier() const -> const boost::uuids::uuid& {
            return identifier_;
        }

        auto write(components::zmq_buffer_t&) -> void;

    private:
        auto init() -> void;

        auto jupyter_engine_init() -> void;

        auto jupyter_kernel_init() -> void;

        std::unique_ptr<actor_zeta::executor::abstract_executor> coordinator_;
        std::vector<actor_zeta::intrusive_ptr<actor_zeta::supervisor>> storage_;
        std::vector<actor_zeta::actor> actor_storage_;

        boost::filesystem::path jupyter_connection_path_;
        components::sandbox_mode_t mode_;
        bool ssh_;
        std::string ssh_host_;
        std::uint16_t ssh_port_;
        std::unique_ptr<components::ssh_forwarder_t> ssh_forwarder_;
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
        std::unique_ptr<zmq::socket_t> iopub_socket;
        std::unique_ptr<zmq::socket_t> heartbeat_socket;
        std::unique_ptr<zmq::socket_t> registration_socket;
        bool engine_mode;
        boost::uuids::uuid identifier_;
        std::vector<std::function<void()>> init_;
    };

    template<
        typename Actor,
        typename Manager,
        typename... Args>
    auto make_service(actor_zeta::intrusive_ptr<Manager>& manager, Args&&... args) {
        return manager->join(new Actor(manager, std::forward<Args>(args)...));
    }

    template<
        typename Root,
        typename Manager,
        typename... Args>
    auto make_manager_service(actor_zeta::intrusive_ptr<Root> app, Args&&... args) {
        actor_zeta::intrusive_ptr<Manager> tmp(
            new Manager(
                app,
                std::forward<Args>(args)...));
        app.join(tmp);
        return tmp;
    }

    template<
        typename Manager,
        typename... Args>
    auto make_manager_service(Args&&... args) {
        return actor_zeta::intrusive_ptr<Manager>(
            new Manager(
                std::forward<Args>(args)...));
    }

} // namespace services
