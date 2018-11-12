#pragma once

#include <boost/beast/core/flat_buffer.hpp>

#include <rocketjoe/api/http.hpp>
#include <rocketjoe/data_provider/http/http_context.hpp>
#include <rocketjoe/data_provider/http/websocket_session.hpp>
#include <rocketjoe/api/http.hpp>

namespace rocketjoe { namespace data_provider { namespace http_server {

            using tcp = boost::asio::ip::tcp;               // from <boost/asio/ip/tcp.hpp>
            namespace http = boost::beast::http;            // from <boost/beast/http.hpp>
            namespace websocket = boost::beast::websocket;  // from <boost/beast/websocket.hpp>

            class http_session final : public std::enable_shared_from_this<http_session> {
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

                    http_session &self_;
                    std::vector<std::unique_ptr<work>> items_;

                public:
                    explicit queue(http_session &self);

                    // Returns `true` if we have reached the queue_vm limit
                    bool is_full() const;

                    // Called when a message finishes sending
                    // Returns `true` if the caller should initiate a read
                    bool on_write();

                    // Called by the HTTP handler to send a response.
                    template<bool isRequest, class Body, class Fields>
                    void operator()(http::message <isRequest, Body, Fields> &&msg) {
                        // This holds a work item
                        struct work_impl final : work {
                            http_session &self_;
                            http::message <isRequest, Body, Fields> msg_;

                            work_impl(
                                    http_session &self,
                                    http::message <isRequest, Body, Fields> &&msg)
                                    : self_(self), msg_(std::move(msg)) {
                            }

                            void
                            operator()() {
                                http::async_write(
                                        self_.socket_,
                                        msg_,
                                        boost::asio::bind_executor(
                                                self_.strand_,
                                                std::bind(
                                                        &http_session::on_write,
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

                tcp::socket socket_;
                boost::asio::strand<boost::asio::io_context::executor_type> strand_;
                boost::asio::steady_timer timer_;
                boost::beast::flat_buffer buffer_;
                http::request <http::string_body> req_;
                queue queue_;
                http_context& handle_processing;
                const api::transport_id id;
            public:
                http_session(tcp::socket socket,api::transport_id , http_context& );

                ~http_session();

                // Start the asynchronous operation
                void run();

                void do_read();

                // Called when the timer expires.
                void on_timer(boost::system::error_code ec);

                void on_read(boost::system::error_code ec);

                void on_write(boost::system::error_code ec, bool close);

                void do_close();

                void write(std::unique_ptr<api::transport_base>);

            };

}}}