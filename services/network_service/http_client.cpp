#include "http_client.hpp"
#include "tracy/tracy.hpp"

#include "routes.hpp"

http_client_t::http_client_t(net::io_context& ioc, log_t& log)
    : log_(log.clone())
    , work_(boost::asio::make_work_guard(ioc))
    , tcp_resolver_(net::make_strand(ioc))
    , stream_(net::make_strand(ioc)) {
    ZoneScoped;
}

void http_client_t::send_loop() {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    if (!tx_is_empty()) {
        auto& task = tx_front();
        stream_.expires_after(std::chrono::seconds(30));
        stream_.async_connect(task.results_, beast::bind_front_handler(&http_client_t::on_connect, this));
    }
}

void http_client_t::on_connect(beast::error_code ec, tcp_resolver_endpoint_t) {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    if (ec) {
        log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
        dequeue();
        post(stream_.get_executor(), [this] { send_loop(); });
        return;
    }

    stream_.expires_after(std::chrono::seconds(30));
    if (!tx_is_empty()) {
        auto& task = tx_front();
        http::async_write(stream_, task.request_, beast::bind_front_handler(&http_client_t::on_write, this));
    }
}

void http_client_t::on_write(beast::error_code ec, std::size_t bytes_transferred) {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    boost::ignore_unused(bytes_transferred);

    if (ec) {
        log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
        dequeue();
        return;
    }

    http::async_read(stream_, buffer_, response_, beast::bind_front_handler(&http_client_t::on_read, this));
}

void http_client_t::on_read(beast::error_code ec, std::size_t bytes_transferred) {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    boost::ignore_unused(bytes_transferred);

    if (ec) {
        log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
        dequeue();
        return;
    }

    // Gracefully close the socket
    stream_.socket().shutdown(tcp::socket::shutdown_both, ec);

    // not_connected happens sometimes so don't bother reporting it.
    if (ec && ec != beast::errc::not_connected) {
        log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
        dequeue();
        return;
    }
    if (!tx_is_empty()) {
        auto& task = tx_front();
        actor_zeta::send(
            task.address_,
            actor_zeta::actor_address(),
            network_service_routes::dispatch_output,
            std::move(response_));
    }
    log_.trace("{} : Going to dequeue ... .. .", GET_TRACE());
    if (dequeue()) {
        log_.trace("{} : calling send_loop ", GET_TRACE());
        send_loop();
    } else {
        log_.debug("{} : not send_loop: no tasks", GET_TRACE());
    }
}

void http_client_t::send(
    tcp_resolver_results_t&& results,
    request_t&& request,
    const actor_zeta::actor_address& address) {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    task_t task(std::move(results), std::move(request), address);

    post(stream_.get_executor(), [this, task = std::move(task)] {
        if (enqueue(std::move(const_cast<task_t&&>(task)))) {
            send_loop();
        } else {
            log_.debug("{} : not enqueue ", GET_TRACE());
        }
    });
}

http_client_t::~http_client_t() {
    ZoneScoped;
    work_.reset();
}

// Add new element to the end of dequeue
bool http_client_t::enqueue(task_t&& task) {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    tx_emplace_back(std::move(task));
    return !tx_is_empty();
}

// Remove processed element from the front of dequeue
bool http_client_t::dequeue() {
    ZoneScoped;
    log_.trace("{} +++", GET_TRACE());
    if (!tx_is_empty())
        tx_pop_front();
    return !tx_is_empty();
}
