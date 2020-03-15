#pragma once

#include <memory>

#include <rocketjoe/network/network.hpp>
#include "forward.hpp"

namespace rocketjoe { namespace network {

        class http_session final : public std::enable_shared_from_this<http_session> {
            // This queue is used for HTTP pipelining.
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

                // Returns `true` if we have reached the queue limit
                bool is_full() const {
                    return items_.size() >= limit;
                }

                // Called when a message finishes sending
                // Returns `true` if the caller should initiate a read
                bool on_write();

                // Called by the HTTP handler to send a response.
                template<bool isRequest, class Body, class Fields>
                void operator()(network::http::message<isRequest, Body, Fields> &&msg) {
                    // This holds a work item
                    struct work_impl final : work {
                        http_session &self_;
                        network::http::message<isRequest, Body, Fields> msg_;

                        work_impl(
                                http_session &self,
                                network::http::message<isRequest, Body, Fields> &&msg)
                                : self_(self)
                                , msg_(std::move(msg))
                        {
                        }

                        void operator()() {
                            network::http::async_write(
                                    self_.stream_,
                                    msg_,
                                    network::beast::bind_front_handler(
                                            &http_session::on_write,
                                            self_.shared_from_this(),
                                            msg_.need_eof()
                                    )
                            );
                        }
                    };

                    // Allocate and store the work
                    items_.push_back(std::make_unique<work_impl>(self_, std::move(msg)));

                    // If there was no previous work, start this one
                    if (items_.size() == 1) {
                        (*items_.front())();
                    }
                }
            };

            network::beast::tcp_stream stream_;
            network::beast::flat_buffer buffer_;
            queue queue_;
            boost::optional<network::http::request_parser<network::http::string_body>> parser_;
            network::helper_write_f_t handle_processing;
            const std::size_t id;
        public:
            http_session(network::tcp::socket&&, std::size_t, network::helper_write_f_t);

            ~http_session() = default;

            void run();

            void do_read();

            void on_read(network::beast::error_code, std::size_t );

            void on_write(bool close, network::beast::error_code ec, std::size_t bytes_transferred);

            void do_close();

            void write(network::response_type &&);

        };

}}