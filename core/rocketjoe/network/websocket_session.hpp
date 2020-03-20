#pragma once

#include <memory>

#include <rocketjoe/network/network.hpp>
#include "forward.hpp"

namespace rocketjoe { namespace network {

        class websocket_session final : public std::enable_shared_from_this<websocket_session> {
                network::websocket::stream<network::beast::tcp_stream> ws_;
                network::beast::flat_buffer buffer_;
                network::helper_write_f_t pipe_;

            public:
                websocket_session(network::tcp::socket&& socket, network::helper_write_f_t pipe_);

                // Start the asynchronous operation
                template<class Body, class Allocator>
                void do_accept(network::http::request<Body, network::http::basic_fields<Allocator>> req) {
                    // Set suggested timeout settings for the websocket
                    ws_.set_option(network::websocket::stream_base::timeout::suggested(network::beast::role_type::server));

                    // Set a decorator to change the Server of the handshake
                    ////ws_.set_option(network::websocket::stream_base::decorator());

                    // Accept the websocket handshake
                    ws_.async_accept(
                            req,
                            network::beast::bind_front_handler(
                                    &websocket_session::on_accept,
                                    shared_from_this()));
                }

                void on_accept(boost::system::error_code ec);

                void do_read();

                void on_read(network::beast::error_code ec, std::size_t bytes_transferred);

                void on_write(network::beast::error_code ec, std::size_t bytes_transferred);
            };
}}