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
        zmq_buffer_t()
            : id_(-1) {}
        zmq_buffer_t(const zmq_buffer_t&) = default;
        zmq_buffer_t& operator=(const zmq_buffer_t&) = delete;

        zmq_buffer_t(int fd, std::vector<std::string> msgs)
            : id_(fd)
            , msg_(std::move(msgs)) {}

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

    using sender_t = std::function<void(zmq_buffer_t)>;

    class zmq_server_t final {
    public:
        zmq_server_t();

        ~zmq_server_t();

        void run();

        auto write(zmq_buffer_t& buffer) -> void;

        auto add_listener(zmq::pollitem_t client, sender_t);

    private:
        void run_();
        void stop();
        void inner_write(zmq_buffer_t);
        std::mutex mtx_;
        std::thread thread_;
        std::queue<zmq_buffer_t> task_;
        std::queue<zmq_buffer_t> inner_task_;
        std::unordered_map<int, int> fd_index_;
        std::atomic_bool enabled_;
        std::vector<zmq::pollitem_t> polls_table_;
        std::vector<sender_t> senders_;
    };

    class zmq_client_t final {
    public:
        auto add_client(zmq::pollitem_t client) -> int;
        auto write(zmq_buffer_t& buffer) -> void;

    private:
        std::map<int, int> fd_index_;
        std::vector<zmq::pollitem_t> clients_;
        std::vector<sender_t> senders_;
    };

    template<class Url, class Listener, class Adrress>
    void make_listener_zmq_socket(
        Listener& storage,
        const Url& url,
        zmq::socket_type socket_type,
        Adrress& adrress) {
        zmq::socket_t socket{storage->ctx(), socket_type};
        socket.setsockopt(ZMQ_LINGER, 1000);
        socket.bind(url);
        storage->add_listener({socket, 0, ZMQ_POLLIN, 0}, adrress->name());
    }

    template<class Url, class Clients>
    void make_client_zmq_socket(
        Clients& storage,
        Url& url,
        zmq::socket_type socket_type) {
        zmq::socket_t socket{storage->ctx(), socket_type};
        socket.setsockopt(ZMQ_LINGER, 1000);
        socket.bind(url);
        storage.add_client({socket, 0, ZMQ_POLLIN, 0});
    }

    template<class Transport, class IP, class Port>
    auto make_url(Transport&& t, IP&& ip, Port&& port) {
        return fmt::format("{0}://{1}:{2}", t, ip, port);
    }

    template<class Interface, class Port>
    auto make_connect_string(Interface& t, Port& port) {
        return fmt::format("{0}:{1}", t, port);
    }

    class zmq_hub_t final : public goblin_engineer::abstract_manager_service {
    public:
        zmq_hub_t(
            goblin_engineer::components::root_manager*,
            components::log_t&,
            std::unique_ptr<zmq::context_t>);

        ~zmq_hub_t() override = default;

        void enqueue(goblin_engineer::message, actor_zeta::execution_device*) override;

        zmq::context_t& ctx();

        auto write(zmq_buffer_t& buffer) -> void;

        auto add_client(zmq::pollitem_t client);

        auto add_listener(zmq::pollitem_t, actor_zeta::detail::string_view) -> void;

    private:
        std::unique_ptr<zmq::context_t> ctx_;
        std::unordered_set<int> clients_;
        std::unordered_set<int> listener_;
        zmq_client_t zmq_client_;
        zmq_server_t zmq_server_;
    };
} // namespace services
