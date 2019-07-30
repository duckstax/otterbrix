#pragma once

#include <boost/beast/core/flat_buffer.hpp>

#include <rocketjoe/services/http_server/context.hpp>
#include <rocketjoe/services/http_server/websocket_session.hpp>
#include <rocketjoe/network/query_context.hpp>

namespace rocketjoe { namespace http {

        class session final : public std::enable_shared_from_this<session> {
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

                session &self_;
                std::vector<std::unique_ptr<work>> items_;

            public:
                explicit queue(session &self): self_(self) {
                    static_assert(limit > 0, "queue limit must be positive");
                    items_.reserve(limit);
                }

                // Returns `true` if we have reached the queue limit
                bool is_full() const {
                    return items_.size() >= limit;
                }

                // Called when a message finishes sending
                // Returns `true` if the caller should initiate a read
                bool on_write() {
                    assert(!items_.empty());
                    auto const was_full = is_full();
                    items_.erase(items_.begin());
                    if (!items_.empty())
                        (*items_.front())();
                    return was_full;
                }

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

                        void operator()() {
                            http::async_write(
                                    self_.stream_,
                                    msg_,
                                    beast::bind_front_handler(
                                            &session::on_write,
                                            self_.shared_from_this(),
                                            msg_.need_eof()));
                        }
                    };

                    // Allocate and store the work
                    items_.push_back(std::make_unique<work_impl>(self_, std::move(msg)));

                    // If there was no previous work, start this one
                    if (items_.size() == 1)
                        (*items_.front())();
                }
            };

            beast::tcp_stream stream_;
            beast::flat_buffer buffer_;
            queue queue_;
            boost::optional<http::request_parser<http::string_body>> parser_;
            context &handle_processing;
            const std::size_t id;
        public:
            session(tcp::socket&&, std::size_t, context &);

            ~session() = default;

            // Start the asynchronous operation
            void run();

            void do_read();

            void on_read(boost::system::error_code ec);

            void on_write(bool close, beast::error_code ec, std::size_t bytes_transferred);

            void do_close();

            void write(response_type &&);

            auto socket() -> tcp::socket &;

        };

    }
}