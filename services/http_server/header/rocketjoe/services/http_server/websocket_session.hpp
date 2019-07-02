#pragma once

#include <boost/asio.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <rocketjoe/services/http_server/context.hpp>
#include <boost/beast/core/tcp_stream.hpp>

namespace rocketjoe { namespace http {
        namespace beast = boost::beast;
        class websocket_session final : public std::enable_shared_from_this<websocket_session> {
                websocket::stream<beast::tcp_stream> ws_;
                beast::flat_buffer buffer_;
                context& pipe_;

            public:
                websocket_session(tcp::socket&& socket, context& pipe_);

                // Start the asynchronous operation
                template<class Body, class Allocator>
                void do_accept(http::request<Body, http::basic_fields<Allocator>> req) {
                    // Set suggested timeout settings for the websocket
                    ws_.set_option(
                            websocket::stream_base::timeout::suggested(
                                    beast::role_type::server));

                    // Set a decorator to change the Server of the handshake
                    ws_.set_option(websocket::stream_base::decorator(
                            [](websocket::response_type& res)
                            {
                                res.set(http::field::server,
                                        std::string(BOOST_BEAST_VERSION_STRING) +
                                        " advanced-server");
                            }));

                    // Accept the websocket handshake
                    ws_.async_accept(
                            req,
                            beast::bind_front_handler(
                                    &websocket_session::on_accept,
                                    shared_from_this()));
                }

                void on_accept(boost::system::error_code ec);

                void do_read();

                void on_read(beast::error_code ec, std::size_t bytes_transferred);

                void on_write(beast::error_code ec, std::size_t bytes_transferred);
            };
}}