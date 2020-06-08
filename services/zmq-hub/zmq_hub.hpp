#pragma once

#include <atomic>
#include <memory>
#include <queue>
#include <unordered_set>

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
        zmq_server_t();

        ~zmq_server_t();

        void run();

        auto write(zmq_buffer_t&buffer) -> void;

        auto add_lisaner(zmq::pollitem_t client);

    private:
        void stop();

        std::unordered_map<int,int> fd_index_;
        std::atomic_bool enabled_;
        std::vector<zmq::pollitem_t> polls_table_;
    };

    class zmq_client_t final {
    public:
        auto add_client(zmq::pollitem_t client) -> int;
        auto write(zmq_buffer_t&buffer) -> void ;
    private:
        std::map<int,int> fd_index_;
        std::vector<zmq::pollitem_t> clients_;
    };

    template <class Url,class Lisaners >
    void make_lisaner_zmq_socket(
            zmq::context_t&ctx,
            Lisaners&storage,
            Url&url,
            zmq::socket_type  socket_type) {
        zmq::socket_t socket{ctx, socket_type};
        socket.setsockopt(ZMQ_LINGER, 1000);
        socket.bind(url);
        storage->add_client({socket, 0, ZMQ_POLLIN, 0});
    }

    template <class Url, class Clients>
    void make_client_zmq_socket(
            zmq::context_t&ctx,
            Clients&storage,
            Url&url,
            zmq::socket_type  socket_type) {
        zmq::socket_t socket{ctx, socket_type};
        socket.setsockopt(ZMQ_LINGER, 1000);
        socket.bind(url);
        storage.add_client({socket, 0, ZMQ_POLLIN, 0});
    }


    template<class Transport,class IP,class Port>
    auto make_url(const Transport&t,const IP&ip,const Port&port) {
        return fmt::format("{0}://{1}:{2}",t,ip,port);
    }

    template<class Interface,class Port>
    auto make_connect_string(Interface&t,Port&port) {
        return fmt::format("{0}:{1}",t,port);
    }

    class zmq_hub_t final : public goblin_engineer::abstract_manager_service {
    public:
        zmq_hub_t(
                goblin_engineer::components::root_manager*,
                components::log_t&,
                std::unique_ptr<zmq::context_t>);

        ~zmq_hub_t() override = default;

        void enqueue(goblin_engineer::message, actor_zeta::execution_device*) override;

        auto write(zmq_buffer_t&buffer) -> void;

        auto add_client(zmq::pollitem_t client);

        auto add_lisaner(zmq::pollitem_t client);
    private:
        std::unordered_set<int> clients_;
        std::unordered_set<int> lisaner_;
        zmq_client_t zmq_client_;
        zmq_server_t zmq_server_;
    };
} // namespace storage
