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

    class jupyter  final : public actor_zeta::supervisor {
    public:
        jupyter() = delete;
        jupyter(const jupyter&) = delete;
        jupyter& operator=(const jupyter&) = delete;

        jupyter( const components::configuration&, components::log_t&);

        auto executor() noexcept -> actor_zeta::abstract_executor&;

        auto join(actor_zeta::actor) -> actor_zeta::actor_address ;

        auto join(actor_zeta::intrusive_ptr<actor_zeta::supervisor> tmp) -> actor_zeta::actor_address;

        ~jupyter();

        zmq::context_t& zmq_context();

        auto start() -> void;

        auto init() -> void;

        void enqueue(goblin_engineer::message, actor_zeta::execution_device*) override;

        auto write(const std::string&, std::vector<std::string>&) -> void;

        auto pre_init(std::function<void()>)-> void;

    private:
        auto jupyter_engine_init() -> void;

        auto jupyter_kernel_init() -> void;

        std::unique_ptr<actor_zeta::executor::abstract_executor> coordinator_;
        std::vector<actor_zeta::intrusive_ptr<actor_zeta::supervisor>> storage_;
        std::vector<actor_zeta::actor> actor_storage_;

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
        std::unique_ptr<zmq::socket_t> iopub_socket;
        std::unique_ptr<zmq::socket_t> heartbeat_socket;
        std::unique_ptr<zmq::socket_t> registration_socket;
        bool engine_mode;
        std::vector<std::function<void()>> init_;
    };

    template<
        typename Actor,
        typename Manager,
        typename... Args
    >
    auto make_service(actor_zeta::intrusive_ptr<Manager>&manager, Args&&... args){
        return manager->join(new Actor(manager,std::forward<Args>(args)...));
    }


    template<
        typename Root,
        typename Manager,
        typename... Args
    >
    auto make_manager_service(actor_zeta::intrusive_ptr<Root> app,Args&&... args){
        actor_zeta::intrusive_ptr<Manager> tmp(
            new Manager(
                app,
                std::forward<Args>(args)...
            )
        );
        app.join(tmp);
        return tmp;
    }

} // namespace services
