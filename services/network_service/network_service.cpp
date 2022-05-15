#include "network_service.hpp"
#include "tracy/tracy.hpp"
#include "detect_session.hpp"
#include "log/log.hpp"
#include "server_certificate.hpp"
#include <exception>

#include "core/excutor.hpp"
#include "plain_websocket_session.hpp"

constexpr bool reuse_address = true;
using wrk_shared = actor_zeta::executor_t<actor_zeta::work_sharing>;

network_service_t::network_service_t(
    std::string name,
    net::io_context& ioc,
    tcp::endpoint endpoint,
    const clients_t& clients,
    size_t num_workers,
    size_t max_throughput,
    log_t& log,
    address_book addresses)
    : goblin_engineer::abstract_manager_service(network_service_routes::name)
    , coordinator_(new wrk_shared(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter())
    , io_context_(ioc)
    , ioc_read_(net::make_strand(ioc))
    , ioc_write_(net::make_strand(ioc))
    , acceptor_(ioc, endpoint, reuse_address)
    , http_client_(log)
    , server_session_handler_(nullptr)
    , client_session_handler_(nullptr)
    , log_(log.clone()) {
    ZoneScoped;
    load_server_certificate(ssl_ctx_);

    add_handler(network_service_routes::close_session, &network_service_t::close_session);
    add_handler(network_service_routes::http_write, &network_service_t::http_write);
    add_handler(network_service_routes::ws_write, &network_service_t::ws_write);
    add_handler(network_service_routes::http_client_write, &network_service_t::http_client_write);

    server_session_handler_ = std::make_unique<session_handler_t>(
        addresses,
        network_service_routes::dispatcher,
        network_service_routes::http_dispatch,
        network_service_routes::ws_dispatch,
        log);

    if (!clients.empty()) {
        client_session_handler_ = std::make_unique<session_handler_t>(
            addresses,
            network_service_routes::dispatcher,
            network_service_routes::http_dispatch,
            network_service_routes::ws_client_dispatch,
            log);
        for (const auto& client : clients) {
            switch (client.type_) {
                case client_t::type_t::PLAIN_WS: {
                    log_.trace("{} :: Add CLIENT {}:{}:{}", GET_TRACE(), client.host_, client.port_, client.init_message_);
                    client_session_handler_->ws_session(
                                               make_websocket_client_session(
                                                   client_session_handler_.get(),
                                                   http_client_.io_context(),
                                                   log_))
                        .run(client.host_, client.port_, client.init_message_);
                    break;
                }
                default:
                    break;
            }
        }
    } else {
        log_.info("{} :: NO CLIENTS", GET_TRACE());
    }

    coordinator_->start();
    run();
}

network_service_t::network_service_t(
    std::string name,
    net::io_context& ioc,
    tcp::endpoint endpoint,
    const clients_t& clients,
    size_t num_workers,
    size_t max_throughput,
    log_t& log)
    : network_service_t(
          std::move(name),
          ioc,
          endpoint,
          clients,
          num_workers,
          max_throughput,
          log,
          [this](const actor_zeta::detail::string_view& name) -> actor_zeta::actor_address {
              if ("self" == name) {
                  return self();
              }
              auto bak = actor_index_.load();
              this->actor_index_++;
              if (this->actor_index_.load() == this->actor_storage_.size()) {
                  this->actor_index_ = 0;
              }
              this->log_.debug("address lbd, returning:{} address'", bak);
              return this->actor_storage_.at(bak).address();
          }) {}

network_service_t::~network_service_t() {
    ZoneScoped;
    acceptor_.close();
}

void network_service_t::run() {
    ZoneScoped;
    log_.trace("{} :: +++", GET_TRACE());
    do_accept_();
}

void network_service_t::do_accept_() {
    ZoneScoped;
    acceptor_.async_accept(
        ioc_read_,
        beast::bind_front_handler(&network_service_t::on_accept, this));
}

void network_service_t::on_accept(beast::error_code ec, tcp::socket socket) {
    ZoneScoped;
    if (ec) {
        log_.error("{} :: !!! {} !!!", GET_TRACE(), ec.message());
    } else {
        log_.trace("{} :: accept success", GET_TRACE());
        auto session = make_detect_session(server_session_handler_.get(), ssl_ctx_, std::move(socket), log_);
        if (session)
            session->run();
        log_.trace("{} :: OUT OF THE SCOPE OF auto session = make_detect_session !!!", GET_TRACE());
    }
    do_accept_();
}

void network_service_t::enqueue_base(ge::message_ptr msg, actor_zeta::execution_device*) {
    ZoneScoped;
    log_.trace("{} :: messages_.size() = {} +++", GET_TRACE(), messages_.size());
    messages_.emplace_back(std::move(msg));
    net::post(
        ioc_write_,
        [this]() mutable {
            ZoneScoped;
            log_.trace("{} mutable lambda's messages_.size() = {} +++", GET_TRACE(), messages_.size());
            if (!messages_.empty()) {
                log_.trace("{} :: {}", GET_TRACE(), messages_.front()->command());
                ge::message_ptr ptr(messages_.front().get());
                set_current_message(std::move(ptr));
                execute(*this);
                messages_.pop_front();
            }
            log_.trace("{} :: ---", GET_TRACE());
        });
}

auto network_service_t::executor() noexcept -> actor_zeta::abstract_executor* {
    ZoneScoped;
    return coordinator_.get();
}

void network_service_t::http_client_write(
    tcp::resolver::results_type&& results,
    request_t&& request) {
    ZoneScoped;
    log_.trace("{} :: +++", GET_TRACE());

    auto ctx = static_cast<actor_zeta::context*>(this); //hack
    auto client_ptr = http_client_.http_client();
    if (client_ptr)
        client_ptr->send(std::move(results), std::move(request), ctx->current_message()->sender());
    else
        log_.error("{} :: !!! No HTTP CLIENT added: ignore {} !!!", GET_TRACE(), __func__);
}

auto network_service_t::get_executor() noexcept -> ge::abstract_executor* {
    ZoneScoped;
    return coordinator_.get();
}

void network_service_t::http_write(session_id& id, response_t& response) {
    ZoneScoped;
    log_.trace("{} :: id {}", GET_TRACE(), id);

    auto ptr = server_session_handler_->http_session(id);
    if (ptr) {
        ptr->write(std::move(response));
    } else {
        log_.error("{} :: !!! No session {} found !!!", GET_TRACE(), id);
    }
}

void network_service_t::ws_write(session_id& id, ws_message_ptr message) {
    ZoneScoped;
    log_.trace("{} :: id {}", GET_TRACE(), id);

    auto ptr = server_session_handler_->ws_session(id);
    if (ptr) {
        ptr->write(message);
    } else {
        log_.error("{} :: !!! No session {} found !!!", GET_TRACE(), id);
    }
}

void network_service_t::close_session(session_id& id) {
    ZoneScoped;
    log_.trace("{} :: id {}", GET_TRACE(), id);

    if (server_session_handler_->close_session(id))
        return;
}

void network_service_t::ws_broadcast(std::string& data) {
    ZoneScoped;
    log_.trace("{} :: +++", GET_TRACE());

    server_session_handler_->broadcast(std::move(data));
}
