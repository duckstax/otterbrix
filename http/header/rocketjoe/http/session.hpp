#pragma once

#include <boost/beast/core/flat_buffer.hpp>

#include <rocketjoe/http/context.hpp>
#include <rocketjoe/http/websocket_session.hpp>
#include <rocketjoe/http/query_context.hpp>

namespace rocketjoe { namespace http {

    class session final : public std::enable_shared_from_this<session> {
        // This queue_vm is used for HTTP pipelining.
        class queue final {
            enum {
                // Maximum number of responses we will queue
                        limit = 8
            };

            // The type-erased, saved work item
            struct work {
                virtual ~work() = default;

                virtual void operator()() = 0;
            };

            session &self_;
            std::vector<std::unique_ptr<work>> items_;

        public:
            explicit queue(session &self);

            // Returns `true` if we have reached the queue_vm limit
            bool is_full() const;

            // Called when a message finishes sending
            // Returns `true` if the caller should initiate a read
            bool on_write();

            // Called by the HTTP handler to send a response.
            template<bool isRequest, class Body, class Fields>
            void operator()(http::message<isRequest, Body, Fields> &&msg) {
                // This holds a work item
                struct work_impl final : work {
                    session &self_;
                    http::message<isRequest, Body, Fields> msg_;

                    work_impl(
                            session &self,
                            http::message<isRequest, Body, Fields> &&msg)
                            : self_(self), msg_(std::move(msg)) {
                    }

                    void operator()() override {
                        http::async_write(
                                self_.socket_,
                                msg_,
                                boost::asio::bind_executor(
                                        self_.strand_,
                                        std::bind(
                                                &session::on_write,
                                                self_.shared_from_this(),
                                                std::placeholders::_1,
                                                msg_.need_eof())));
                    }
                };

                // Allocate and store the work
                items_.push_back(std::make_unique<work_impl>(self_, std::move(msg)));

                // If there was no previous work, start this one
                if (items_.size() == 1)
                    (*items_.front())();
            }
        };

    private:
        tcp::socket socket_;
        boost::asio::strand<boost::asio::io_context::executor_type> strand_;
        boost::asio::steady_timer timer_;
        boost::beast::flat_buffer buffer_;
        http::request<http::string_body> req_;
        queue queue_;
        context &handle_processing;
        const std::size_t id;
    public:
        session(boost::asio::io_context &io_context, std::size_t, context &);

        ~session() = default;

        // Start the asynchronous operation
        void run();

        void do_read();

        // Called when the timer expires.
        void on_timer(boost::system::error_code ec);

        void on_read(boost::system::error_code ec);

        void on_write(boost::system::error_code ec, bool close);

        void do_close();

        void write(response_type &&);

        auto socket() -> tcp::socket &;

    };

}}