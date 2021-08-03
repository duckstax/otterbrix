#pragma once

#include "beast-compact.hpp"
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "context.hpp"
#include "dto.hpp"
#include "http_client.hpp"
#include "routes.hpp"

#include "log/log.hpp"
#include <goblin-engineer/core.hpp>



namespace ge = goblin_engineer;

namespace detail {
    struct thread_pool_deleter final {
        void operator()(actor_zeta::abstract_executor* ptr) {
            ptr->stop();
            delete ptr;
        }
    };
} // namespace detail

class network_service_t final : public goblin_engineer::abstract_manager_service {
public:
    using address_book = std::function<actor_zeta::actor_address(actor_zeta::detail::string_view)>;

    class client_t final {
    public:
        enum type_t {
            UNKNOWN = -1,
            PLAIN_WS,
            SSL_WS,
        };
        client_t() = default;
        client_t(
            const host_t& host,
            const port_t& port,
            const std::string& init_message,
            type_t type)
            : host_(host)
            , port_(port)
            , init_message_(init_message)
            , type_(type) {}
        /*client_t(const client_t&) = default;
        client_t(client_t&&) = default;*/
        ~client_t() = default;

        host_t host_;
        port_t port_;
        std::string init_message_;
        type_t type_;
    };
    using clients_t = std::vector<client_t>;
    using coord_t = std::unique_ptr<actor_zeta::abstract_executor, detail::thread_pool_deleter>;

    network_service_t(std::string name, net::io_context&, tcp::endpoint, const clients_t&,
                      size_t num_workers, size_t max_throughput, log_t& log);

    network_service_t(std::string name, net::io_context&, tcp::endpoint, const clients_t&,
                      size_t num_workers, size_t max_throughput, log_t& log, address_book addresses);

    ~network_service_t() override;

    void http_write(session_id&, response_t&);
    void ws_write(session_id&, ws_message_ptr);
    void ws_broadcast(std::string&);
    void close_session(session_id&);
    void run();
    void http_client_write(tcp::resolver::results_type&&, request_t&&);
    void enqueue_base(goblin_engineer::message_ptr, actor_zeta::execution_device*) override;
    auto executor() noexcept -> actor_zeta::abstract_executor* override;
    void on_accept(beast::error_code ec, tcp::socket socket);
    auto get_executor() noexcept -> goblin_engineer::abstract_executor* override;

private:
    std::atomic_int actor_index_ = 0;

    coord_t coordinator_;
    net::io_context& io_context_;
    net::strand<net::io_context::executor_type> ioc_read_;
    net::strand<net::io_context::executor_type> ioc_write_;
    tcp::acceptor acceptor_;
    http_client_handler_t http_client_;
    session_handler_ptr server_session_handler_;
    session_handler_ptr client_session_handler_;
    log_t log_;

    ssl::context ssl_ctx_{ssl::context::tlsv12};

    //session_handlers_t session_handlers_;
    std::deque<ge::message_ptr> messages_;

    void do_accept_();
};

using network_service_ptr = boost::intrusive_ptr<network_service_t>;
