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

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/log/log.hpp>

namespace services {

    class zmq_buffer_tt final : public boost::intrusive_ref_counter<zmq_buffer_tt> {
    public:
        zmq_buffer_tt()
            : id_(-1) {}
        zmq_buffer_tt(const zmq_buffer_tt&) = delete;
        zmq_buffer_tt& operator=(const zmq_buffer_tt&) = delete;

        zmq_buffer_tt(int fd, std::vector<std::string> msgs)
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

    using zmq_buffer_t = boost::intrusive_ptr<zmq_buffer_tt>;

    template<class... Args>
    auto buffer(Args&&... args) -> zmq_buffer_t {
        return boost::intrusive_ptr<zmq_buffer_tt>(new zmq_buffer_tt(std::forward<Args>(args)...));
    }

    using sender_t = std::function<void(zmq_buffer_t)>;

    class zmq_server_t final {
    public:
        zmq_server_t();

        ~zmq_server_t();

        void run();

        void stop();

        auto write(zmq_buffer_t& buffer) -> void;

        auto add_listener(std::unique_ptr<zmq::socket_t>, sender_t) -> int;
    private:
        void run_();
        void inner_write(zmq_buffer_t);
        std::mutex mtx_;
        std::thread thread_;
        std::queue<zmq_buffer_t> task_;
        std::queue<zmq_buffer_t> inner_task_;
        std::unordered_map<int, int> fd_index_;
        std::atomic_bool enabled_;
        std::vector<zmq::pollitem_t> polls_table_;
        std::vector<sender_t> senders_;
        std::vector<std::unique_ptr<zmq::socket_t>> original_socket_;
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
        zmq::context_t& ctx,
        Listener& storage,
        const Url& url,
        zmq::socket_type socket_type,
        Adrress& adrress) {
        try {
            auto socket_ = std::make_unique<zmq::socket_t>(ctx, socket_type);
            socket_->setsockopt(ZMQ_LINGER, 1000);
            socket_->bind(url);
            storage->add_listener(std::move(socket_), adrress->name());
        } catch (std::exception const& e) {
            std::cerr << "Run exception : " << e.what() << std::endl;
        }
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
            components::log_t&);

        ~zmq_hub_t() override = default;

        void enqueue(goblin_engineer::message, actor_zeta::execution_device*) override;

        void run();

        void stop();

        auto write(zmq_buffer_t& buffer) -> void;

        auto add_client(zmq::pollitem_t client);

        auto add_listener(std::unique_ptr<zmq::socket_t>, actor_zeta::detail::string_view) -> void;

        auto executor() noexcept -> actor_zeta::abstract_executor& override;

    private:
        std::atomic_bool init_;
        std::unordered_set<int> clients_;
        std::unordered_set<int> listener_;
        zmq_client_t zmq_client_;
        zmq_server_t zmq_server_;
        std::unique_ptr<actor_zeta::executor::abstract_executor> coordinator_;
    };
} // namespace services
