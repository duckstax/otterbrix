#pragma once

#include <memory>
#include <vector>

#include "beast-compact.hpp"
#include "context.hpp"

#include "plain_websocket_session.hpp"
#include "ssl_websocket_session.hpp"

#include "tracy/tracy.hpp"

#include "session.hpp"

auto make_websocket_session(
    session_handler_t* session_handler,
    beast::tcp_stream stream,
    log_t&) -> std::shared_ptr<plain_websocket_session_t>;

auto make_websocket_session(
    session_handler_t* session_handler,
    beast::ssl_stream<beast::tcp_stream> stream,
    log_t&) -> std::shared_ptr<ssl_websocket_session_t>;

template<class Derived>
class http_session_t : public http_session_abstract_t {
    Derived& derived() {
        ZoneScoped;
        return static_cast<Derived&>(*this);
    }

    class queue final {
        enum {
            limit = 8
        };

        // The type-erased, saved work item
        struct work {
            virtual ~work() = default;
            virtual void operator()() = 0;
        };

        http_session_t& self_;
        std::vector<std::unique_ptr<work>> items_;
        log_t log_;

    public:
        explicit queue(http_session_t& self, log_t log)
            : self_(self)
            , log_(log.clone()) {
            ZoneScoped;
            log_.trace("queue::queue, limit:{}", limit);
            static_assert(limit > 0, "queue limit must be positive");
            items_.reserve(limit);
        }

        // Returns `true` if we have reached the queue limit
        bool is_full() const {
            ZoneScoped;
            return items_.size() >= limit;
        }

        auto size() const {
            return items_.size();
        }

        bool on_write() {
            ZoneScoped;
            log_.trace("queue::on_write, queue:{}", items_.size());
            BOOST_ASSERT(!items_.empty());
            auto const was_full = is_full();
            items_.erase(items_.begin());
            if (!items_.empty())
                (*items_.front())();
            log_.trace("queue::on_write2, queue:{}", items_.size());
            return was_full;
        }

        // Called by the HTTP handler to send a response.
        template<bool isRequest, class Body, class Fields>
        void operator()(http::message<isRequest, Body, Fields>&& msg) {
            log_.trace("queue::operator(), queue:{}", items_.size());
            ZoneScoped;
            // This holds a work item
            struct work_impl final : work {
                http_session_t& self_;
                http::message<isRequest, Body, Fields> msg_;

                work_impl(
                    http_session_t& self,
                    http::message<isRequest, Body, Fields>&& msg)
                    : self_(self)
                    , msg_(std::move(msg)) {
                    ZoneScoped;
                }

                void operator()() {
                    ZoneScoped;
                    http::async_write(
                        self_.derived().stream(),
                        msg_,
                        beast::bind_front_handler(
                            &http_session_t::on_write,
                            self_.derived().shared_from_this(),
                            msg_.need_eof()));
                    /*http::async_write(
                        self_.derived().stream(),
                        msg_,
                        net::bind_executor(
                            ctx_->io_context_write(),
                            beast::bind_front_handler(
                                &http_session_t::on_write,
                                self_.derived().shared_from_this(),
                                msg_.need_eof())));*/
                }
            };

            // Allocate and store the work
            items_.emplace_back(std::make_unique<work_impl>(self_, std::move(msg)));
            log_.trace("queue::operator()2, queue:{}", items_.size());

            // If there was no previous work, start this one
            if (items_.size() == 1)
                (*items_.front())();
        }
    };

    session_handler_t* session_handler_;
    queue queue_;
    const std::uintptr_t id_;

    boost::optional<http::request_parser<http::string_body>> parser_;
    response_t response_;

    void write_impl(response_t response) final {
        ZoneScoped;
        response_ = std::move(response);
        queue_(std::move(response_));
    }

protected:
    log_t log_;
    beast::flat_buffer buffer_;

public:
    // Construct the session
    http_session_t(
        session_handler_t* session_handler,
        beast::flat_buffer buffer, log_t& log)
        : session_handler_(session_handler)
        , queue_(*this, log.clone())
        , buffer_(std::move(buffer))
        , id_(reinterpret_cast<std::uintptr_t>(this))
        , log_(log.clone()) {
        ZoneScoped;
    }

    void do_read() {
        ZoneScoped;
        log_.trace("http_session::do_read, queue:{}", queue_.size());
        parser_.emplace();

        parser_->body_limit(1024 * 1024 * 20);

        /*
        beast::get_lowest_layer(
            derived().stream())
            .expires_after(std::chrono::seconds(30));
            */

        http::async_read(
            derived().stream(),
            buffer_,
            *parser_,
            beast::bind_front_handler(
                &http_session_t::on_read,
                derived().shared_from_this()));

        /*http::async_read(
            derived().stream(),
            buffer_,
            *parser_,
            net::bind_executor(
                ctx_->io_context_read(),
                beast::bind_front_handler(
                    &http_session_t::on_read,
                    derived().shared_from_this())));*/
    }

    void on_read(beast::error_code ec, std::size_t bytes_transferred) {
        ZoneScoped;
        log_.trace("http_session::on_read, queue:{}", queue_.size());
        boost::ignore_unused(bytes_transferred);

        // This means they closed the connection
        if (ec == http::error::end_of_stream) {
            log_.trace("http_session::on_read, eof");
            derived().do_eof();
            session_handler_->close(id_);
            return;
        }

        if (ec) {
            log_.error("read : {}", ec.message());
            session_handler_->close(id_);
            return;
        }

        // See if it is a WebSocket Upgrade
        if (websocket::is_upgrade(parser_->get())) {
            ///beast::get_lowest_layer(derived().stream()).expires_never();

            session_handler_->ws_session(
                    make_websocket_session(
                        session_handler_, derived().release_stream(), log_))
                .run(std::move(parser_->release()));
            session_handler_->close(id_);
            return;
        }

        // Send the response

        session_handler_->http_dispatch(id_, std::move(parser_->release()));

        // If we aren't at the queue limit, try to pipeline another request
        if (!queue_.is_full()) {
            do_read();
        }
    }

    void on_write(bool close, beast::error_code ec, std::size_t bytes_transferred) {
        ZoneScoped;
        log_.trace("http_session::on_write, queue:{}", queue_.size());
        boost::ignore_unused(bytes_transferred);

        if (ec) {
            log_.error("write : {}", ec.message());
            session_handler_->close(id_);
            return;
        }

        if (close) {
            log_.trace("http_session::on_write, close");
            derived().do_eof();
            session_handler_->close(id_);
            return;
        }

        if (queue_.on_write()) {
            do_read();
        }
    }
};