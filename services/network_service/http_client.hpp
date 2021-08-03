#pragma once

#include <cstdlib>
#include <deque>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <queue>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <goblin-engineer/core.hpp>

#include "dto.hpp"
#include "endpoint.hpp"

#include "log/log.hpp"

namespace beast = boost::beast;   // from <boost/beast.hpp>
namespace http = beast::http;     // from <boost/beast/http.hpp>
namespace net = boost::asio;      // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp; // from <boost/asio/ip/tcp.hpp>
using tcp_resolver_results_t = tcp::resolver::results_type;
using tcp_resolver_endpoint_t = tcp::resolver::results_type::endpoint_type;

class http_client_t final {
public:
    http_client_t(net::io_context& ioc, log_t& log);
    ~http_client_t();

    void send(
        tcp_resolver_results_t&&,
        request_t&&,
        const actor_zeta::actor_address&);

private:
    void send_loop();
    void on_connect(beast::error_code, tcp_resolver_endpoint_t);
    void on_write(beast::error_code, std::size_t);
    void on_read(beast::error_code, std::size_t);

    class task_t final {
    public:
        task_t(
            tcp_resolver_results_t&& results,
            request_t&& request,
            const actor_zeta::actor_address& address)
            : results_(std::move(results))
            , request_(std::move(request))
            , address_(address) {}
        task_t(task_t&&) = default;
        task_t(const task_t&) = default;
        ~task_t() = default;

        tcp_resolver_results_t results_;
        request_t request_;
        actor_zeta::actor_address address_;
    };

    bool enqueue(task_t&& task);
    bool dequeue();

    log_t log_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
    tcp::resolver tcp_resolver_;
    beast::tcp_stream stream_;
    beast::flat_buffer buffer_;
    response_t response_;

    std::deque<task_t> tx_;

    size_t tx_size() const {
        return tx_.size();
    }

    bool tx_is_empty() const {
        return tx_.empty();
    }

    task_t& tx_back() {
        assert(!tx_is_empty());
        return tx_.back();
    }

    task_t& tx_front() {
        assert(!tx_is_empty());
        return tx_.front();
    }

    void tx_pop_front() {
        assert(!tx_is_empty());
        tx_.pop_front();
    }

    void tx_pop_back() {
        assert(!tx_is_empty());
        tx_.pop_back();
    }

    void tx_emplace_front(task_t&& task) {
        tx_.emplace_front(std::move(task));
    }

    void tx_emplace_back(task_t&& task) {
        tx_.emplace_back(std::move(task));
    }
};

using http_client_ptr = std::unique_ptr<http_client_t>;

class http_client_handler_t final {
    void run() {
        ZoneScoped;
        thr_ = std::thread([this]() {
            ioc_.run();
        });
        thr_started_ = true;
    }

public:
    http_client_handler_t(log_t& log)
        : thr_started_(false) {
        ZoneScoped;
        client_ = std::make_unique<http_client_t>(ioc_, log);
        run();
    }
    ~http_client_handler_t() {
        ZoneScoped;
        if (thr_started_) {
            ioc_.stop();
            thr_.join();
        }
    }

    auto http_client() -> http_client_t* {
        ZoneScoped;
        return client_.get();
    }

    auto io_context() -> net::io_context& {
        ZoneScoped;
        return ioc_;
    }

private:
    bool thr_started_;
    http_client_ptr client_;

    net::io_context ioc_;
    std::thread thr_;
};
