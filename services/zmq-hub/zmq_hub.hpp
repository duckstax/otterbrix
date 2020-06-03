#pragma once

#include <atomic>
#include <memory>
#include <queue>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <goblin-engineer/abstract_manager_service.hpp>
#include <goblin-engineer/components/root_manager.hpp>

#include <components/log/log.hpp>

namespace services {

    class zmq_buffer_t final {
    public:
        zmq_buffer_t(const zmq_buffer_t&) = delete ;
        zmq_buffer_t&operator = (const zmq_buffer_t&) = delete ;

        int id() const {
            return id_;
        }

        const std::vector<std::string>& msg() const {
            return msg_;
        }

    private:
        std::vector<std::string> msg_;
       int id_;
    };

    class zmq_server_t final {
    public:
        zmq_server_t(std::vector<zmq::pollitem_t> polls,int io_threads = 0,int max_sockets = ZMQ_MAX_SOCKETS_DFLT );

        ~zmq_server_t();

        void run();

        auto write(zmq_buffer_t&buffer) -> void;

    private:
        void stop() {
            enabled_ = false;
            zmq_context_->close();
        }

        std::unordered_map<int,int> fd_position_;
        std::atomic_bool enabled_;
        std::unique_ptr<zmq::context_t> zmq_context_;
        std::vector<zmq::pollitem_t> polls_table_;
    };

    class zmq_hub_t final : public goblin_engineer::abstract_manager_service {
    public:
        zmq_hub_t(goblin_engineer::components::root_manager*,  components::log_t&,std::vector<zmq::pollitem_t>);

        ~zmq_hub_t() override = default;

        void enqueue(goblin_engineer::message, actor_zeta::execution_device*) override;

        auto write(zmq_buffer_t&buffer) -> void;
    private:
        zmq_server_t zmq_server_;
    };
} // namespace storage
