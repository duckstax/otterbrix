#pragma once
#include <atomic>
#include <boost/beast/core/buffers_to_string.hpp>
#include <memory>
#include <string>
#include <tracy/tracy.hpp>

#include "beast-compact.hpp"
#include "context.hpp"
#include "log/log.hpp"
#include "session.hpp"

template<class Derived>
class websocket_client_session_t : public ws_session_abstract_t {
public:
    websocket_client_session_t(
        session_handler_t* session_handler,
        log_t& log)
        : session_handler_(session_handler)
        , id_(reinterpret_cast<std::uintptr_t>(this))
        , log_(log.clone()) {
        ZoneScoped;
    }

    ~websocket_client_session_t() = default;

    auto run(
        const host_t& host,
        const port_t& port,
        const std::string& init_message) -> void {
        ZoneScoped;
        log_.trace("{} +++", GET_TRACE());
        host_.assign(host);
        port_.assign(port);
        init_message_.assign(init_message);
        derived().resolver().async_resolve(
            host_,
            port_,
            beast::bind_front_handler(
                &websocket_client_session_t::on_resolve,
                derived().shared_from_this()));
    }

protected:
    void write_impl(ws_message_ptr message) override {
        ZoneScoped;
        log_.trace("{} {} : response.size() = {} +++",
                   GET_TRACE(), id_, message->size());
    }
    //@TODO !!! double close safety
    void close_impl() override {
        ZoneScoped;
        std::string str("exited");
        auto size = str.size();
        session_handler_->ws_dispatch(id_, std::move(str), size);
        buffer_.consume(buffer_.size());
        buffer_.clear();
    }

    bool is_close_impl() override {
        ZoneScoped;
        return false;
    }

private:
    // Access the derived class, this is part of
    // the Curiously Recurring Template Pattern idiom.
    Derived& derived() {
        ZoneScoped;
        return static_cast<Derived&>(*this);
    }

    session_handler_t* session_handler_;
    const std::uintptr_t id_;
    beast::flat_buffer buffer_;
    log_t log_;

    host_t host_;
    port_t port_;
    std::string init_message_;

private:
    void on_resolve(
        beast::error_code ec,
        tcp::resolver::results_type results) {
        ZoneScoped;
        log_.trace("{} +++", GET_TRACE());
        if (ec) {
            log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
            session_handler_->close(id_);
            return;
        }

        // Set the timeout for the operation
        beast::get_lowest_layer(derived().ws()).expires_after(std::chrono::seconds(30));

        // Make the connection on the IP address we get from a lookup
        beast::get_lowest_layer(derived().ws()).async_connect(results, beast::bind_front_handler(&websocket_client_session_t::on_connect, derived().shared_from_this()));
    }

    void on_connect(
        beast::error_code ec,
        tcp::resolver::results_type::endpoint_type ep) {
        ZoneScoped;
        log_.trace("{} +++", GET_TRACE());
        if (ec) {
            log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
            session_handler_->close(id_);
            return;
        }

        // Turn off the timeout on the tcp_stream, because
        // the websocket stream has its own timeout system.
        beast::get_lowest_layer(derived().ws()).expires_never();

        // Set suggested timeout settings for the websocket
        /*derived().ws().set_option(
            websocket::stream_base::timeout::suggested(
                beast::role_type::client));*/

        // Set a decorator to change the User-Agent of the handshake
        derived().ws().set_option(websocket::stream_base::decorator(
            [](websocket::request_type& req) {
                /*req.set(http::field::user_agent,
                    std::string(BOOST_BEAST_VERSION_STRING) +
                        " websocket-client-async");*/
            }));

        websocket::stream_base::timeout opt{
            std::chrono::seconds(30),       // handshake timeout
            websocket::stream_base::none(), // idle timeout. no data recv from peer for 6 sec, then it is timeout
            true                            //enable ping-pong to keep alive
        };

        derived().ws().set_option(opt);

        derived().ws().read_message_max(20ull << 20); // sets the limit to 20 miB

        // Update the host_ string. This will provide the value of the
        // Host HTTP header during the WebSocket handshake.
        // See https://tools.ietf.org/html/rfc7230#section-5.4
        host_ += ':' + std::to_string(ep.port());

        // Perform the websocket handshake
        derived().ws().async_handshake(
            host_, "/",
            beast::bind_front_handler(
                &websocket_client_session_t::on_handshake,
                derived().shared_from_this()));
    }

    void on_handshake(beast::error_code ec) {
        ZoneScoped;
        log_.trace("{} +++", GET_TRACE());
        if (ec) {
            log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
            session_handler_->close(id_);
            return;
        }

        // Send the message
        derived().ws().async_write(
            net::buffer(init_message_),
            beast::bind_front_handler(
                &websocket_client_session_t::on_write,
                derived().shared_from_this()));
    }

    void on_write(
        beast::error_code ec,
        std::size_t bytes_transferred) {
        ZoneScoped;
        log_.trace("{} bytes_transferred = {} +++",
                   GET_TRACE(), bytes_transferred);

        if (ec) {
            log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
            session_handler_->close(id_);
            return;
        }

        // Read a message into our buffer
        derived().ws().async_read(
            buffer_,
            beast::bind_front_handler(
                &websocket_client_session_t::on_read,
                derived().shared_from_this()));
    }

    void on_read(
        beast::error_code ec,
        std::size_t bytes_transferred) {
        ZoneScoped;
        log_.trace("{} bytes_transferred = {} +++", GET_TRACE(), bytes_transferred);

        if (ec) {
            log_.error("{} : !!! Failed: {} !!!", GET_TRACE(), ec.message());
            session_handler_->close(id_);
            return;
        }

        log_.trace("{} buffer_.consume {} bytes", GET_TRACE(), buffer_.size());
        auto buf_str = beast::buffers_to_string(buffer_.data());
        auto buf_size = buf_str.size();
        session_handler_->ws_dispatch(id_, std::move(buf_str), buf_size);
        buffer_.consume(buf_size);

        // Read a message into our buffer
        derived().ws().async_read(
            buffer_,
            beast::bind_front_handler(
                &websocket_client_session_t::on_read,
                derived().shared_from_this()));
    }
};

template<class Derived>
class websocket_session_t : public ws_session_abstract_t {
public:
    websocket_session_t(session_handler_t* session_handler, log_t& log)
        : session_handler_(session_handler)
        , log_(log.clone())
        , id_(reinterpret_cast<std::uintptr_t>(this))
        , is_writing_(false) {
        ZoneScoped;
    }

    // https://github.com/boostorg/beast/blob/develop/example/websocket/server/async/websocket_server_async.cpp
    // Get on the correct executor
    template<class Body, class Allocator>
    auto run(http::request<Body, http::basic_fields<Allocator>> req)
        -> void {
        ZoneScoped;
        // We need to be executing within a strand to perform async operations
        // on the I/O objects in this session. Although not strictly necessary
        // for single-threaded contexts, this example code is written to be
        // thread-safe by default.
        net::dispatch(
            derived().ws().get_executor(),
            beast::bind_front_handler(
                &websocket_session_t::do_accept<Body, Allocator>,
                derived().shared_from_this(),
                std::move(req)));
        //do_accept(std::move(req));
    }

protected:
    void write_impl(ws_message_ptr message) override {
        ZoneScoped;
        if (!message)
            return;
        log_.debug("{} {} : message.size() = {} +++",
                   GET_TRACE(), id_, message->size());
        send(message);
    }
    //@TODO !!! double close safety
    void close_impl() override {
        ZoneScoped;
        std::string str("exited");
        auto size = str.size();
        session_handler_->ws_dispatch(id_, std::move(str), size);
        buffer_.consume(buffer_.size());
        buffer_.clear();
    }

    bool is_close_impl() override {
        ZoneScoped;
        return false;
    }

private:
    // Access the derived class, this is part of
    // the Curiously Recurring Template Pattern idiom.
    Derived& derived() {
        ZoneScoped;
        return static_cast<Derived&>(*this);
    }

    session_handler_t* session_handler_;
    log_t log_;
    const std::uintptr_t id_;
    beast::flat_buffer buffer_;

    std::atomic_bool is_writing_;

    // Start the asynchronous operation
    template<class Body, class Allocator>
    void
    do_accept(http::request<Body, http::basic_fields<Allocator>> req) {
        ZoneScoped;

        // Turn off the timeout on the tcp_stream, because
        // the websocket stream has its own timeout system.
        beast::get_lowest_layer(derived().ws()).expires_never();

        // Standard suggested timeout:
        /*derived().ws().set_option(
            websocket::stream_base::timeout::suggested(
                beast::role_type::server));*/

        derived().ws().set_option(
            websocket::stream_base::decorator(
                [](websocket::response_type& res) {

                }));

        websocket::stream_base::timeout opt{
            std::chrono::seconds(30),       // handshake timeout
            websocket::stream_base::none(), // idle timeout. no data recv from peer for 6 sec, then it is timeout
            true                            //enable ping-pong to keep alive
        };

        derived().ws().set_option(opt);

        derived().ws().read_message_max(20ull << 20); // sets the limit to 20 miB

        derived().ws().async_accept(
            req,
            beast::bind_front_handler(
                &websocket_session_t::on_accept,
                derived().shared_from_this()));
    }

    void on_accept(beast::error_code ec) {
        ZoneScoped;
        if (ec) {
            log_.error("{} {} : !!! {} !!!",
                       GET_TRACE(), id_, ec.message());
            session_handler_->close(id_);
            return;
        }

        // Read a message
        net::post(
            derived().ws().get_executor(),
            beast::bind_front_handler(
                &websocket_session_t::do_read,
                derived().shared_from_this()));
    }

    void do_read() {
        ZoneScoped;
        log_.trace("{} {} +++", GET_TRACE(), id_);
        // Read a message into our buffer
        derived().ws().async_read(
            buffer_,
            beast::bind_front_handler(
                &websocket_session_t::on_read,
                derived().shared_from_this()));

        log_.trace("{} {} ---", GET_TRACE(), id_);
    }

    void on_read(beast::error_code ec,
                 std::size_t bytes_transferred) {
        ZoneScoped;
        log_.trace("{} {} bytes_transferred = {} +++",
                   GET_TRACE(), id_, bytes_transferred);

        // This indicates that the websocket_session was closed
        if (ec == websocket::error::closed) {
            log_.debug("{} {} : the websocket_session was closed",
                       GET_TRACE(), id_);
            session_handler_->close(id_);
            return;
        }

        if (ec) {
            log_.error("{} {} : !!! {} !!!",
                       GET_TRACE(), id_, ec.message());
            dequeue();
            session_handler_->close(id_);
            return;
        }

        auto buf_str = beast::buffers_to_string(buffer_.data());
        auto buf_size = buf_str.size();
        session_handler_->ws_dispatch(id_, std::move(buf_str), buf_size);
        buffer_.consume(buf_size);

        net::post(
            derived().ws().get_executor(),
            beast::bind_front_handler(
                &websocket_session_t::do_read,
                derived().shared_from_this()));
    }

    void
    run_async_write() {
        ZoneScoped;

        log_.trace("{} {} +++", GET_TRACE(), id_);

        if (is_empty())
            return;

        auto& task = front();
        log_.trace("{} {} : derived().ws().async_write {} ... .. .", GET_TRACE(), id_, task.message_->size());

        derived().ws().text(task.message_->is_text());
        derived().ws().async_write(
            net::buffer(task.message_->data(), task.message_->size()),
            beast::bind_front_handler(
                &websocket_session_t::on_write,
                derived().shared_from_this()));
    }

    void
    on_write(
        beast::error_code ec,
        std::size_t bytes_transferred) {
        ZoneScoped;

        log_.trace("{} {} +++", GET_TRACE(), id_);

        if (ec) {
            log_.error("{} {} : !!! {} !!!",
                       GET_TRACE(), id_, ec.message());
            dequeue();
            session_handler_->close(id_);
            return;
        }

        log_.debug("{} {} bytes_transferred = {} +++",
                   GET_TRACE(), id_, bytes_transferred);

        dequeue();
        log_.trace("{} {} ---", GET_TRACE(), id_);

        // https://github.com/boostorg/beast/blob/c4813a5ac79d802951cf70b811ded94ce7ef7d79/example/cppcon2018/websocket_session.cpp#L87
        // https://github.com/boostorg/beast/issues/1381
        net::post(
            derived().ws().get_executor(),
            beast::bind_front_handler(
                &websocket_session_t::run_async_write,
                derived().shared_from_this()));
    }

    class task_t final {
    public:
        task_t(ws_message_ptr message)
            : message_(message) {}
        task_t(task_t&&) = default;
        task_t(const task_t&) = default;
        ~task_t() = default;

        ws_message_ptr message_;
    };

    /**
     * IMPORTANT @NOTE:
     * On multi-async operation catch this:
     * 
     * /include/boost/beast/websocket/detail/soft_mutex.hpp:79
     *  template<class T>
        bool
        try_lock(T const*)
        {
            // If this assert goes off it means you are attempting to
            // simultaneously initiate more than one of same asynchronous
            // operation, which is not allowed. For example, you must wait
            // for an async_read to complete before performing another
            // async_read.
            //
            BOOST_ASSERT(id_ != T::id);
            if(id_ != 0)
                return false;
            id_ = T::id;
            return true;
        }
     * 
     **/

    void send_loop() {
        ZoneScoped;
        log_.trace("{} {} +++", GET_TRACE(), id_);

        // https://github.com/boostorg/beast/blob/c4813a5ac79d802951cf70b811ded94ce7ef7d79/example/cppcon2018/websocket_session.cpp#L87
        // https://github.com/boostorg/beast/issues/1381
        // Are we already writing?
        if (size() > 1)
            return;

        net::post(
            derived().ws().get_executor(),
            beast::bind_front_handler(
                &websocket_session_t::run_async_write,
                derived().shared_from_this()));
    }

    void send(ws_message_ptr message) {
        ZoneScoped;
        log_.trace("{} {} +++", GET_TRACE(), id_);
        task_t task(message);
        if (enqueue(task_t(message))) {
            send_loop();
        } else {
            log_.debug("{} {} : not enqueue ", GET_TRACE(), id_);
        }
    }

    // Add new element to the end of dequeue
    bool enqueue(task_t&& task) {
        ZoneScoped;
        log_.trace("{} : ", GET_TRACE());
        emplace_back(std::move(task));
        return !is_empty();
    }

    bool dequeue() {
        ZoneScoped;
        log_.trace("{} : ", GET_TRACE());
        if (!is_empty())
            pop_front();
        return !is_empty();
    }

    std::deque<task_t> tx_;

    size_t size() const {
        return tx_.size();
    }

    bool is_empty() const {
        return tx_.empty();
    }

    task_t& back() {
        assert(!is_empty());
        return tx_.back();
    }

    task_t& front() {
        assert(!is_empty());
        return tx_.front();
    }

    void pop_front() {
        assert(!is_empty());
        tx_.pop_front();
    }

    void pop_back() {
        assert(!is_empty());
        tx_.pop_back();
    }

    void emplace_front(task_t&& task) {
        tx_.emplace_front(std::move(task));
    }

    void emplace_back(task_t&& task) {
        tx_.emplace_back(std::move(task));
    }
};
