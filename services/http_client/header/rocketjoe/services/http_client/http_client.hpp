#pragma once

#include <memory>
#include <chrono>

#include <rocketjoe/network/network.hpp>

#include <iostream>

namespace rocketjoe { namespace http {
using namespace network::http;
using namespace network::beast;

        class http_client_session final : public std::enable_shared_from_this<http_client_session> {
            network::tcp::resolver resolver_;
            network::beast::tcp_stream stream_;
            network::beast::flat_buffer buffer_; // (Must persist between reads)
            network::http::request<network::http::string_body> req_;
            network::http::response<network::http::string_body> res_;

        public:
            // Objects are constructed with a strand to
            // ensure that handlers do not execute concurrently.
            explicit
            http_client_session(network::net::io_context& ioc):

            resolver_ (network::net::make_strand(ioc))
            , stream_(network::net::make_strand(ioc)) {
            }

            // Start the asynchronous operation
            void
            run(
                    char const *host,
                    char const *port,
                    char const *target) {

                req_.version(11);
                req_.method(network::http::verb::get);
                req_.target(target);
                req_.set(network::http::field::host, host);

                // Look up the domain name
                resolver_.async_resolve(
                        host,
                        port,
                        network::beast::bind_front_handler(
                                &http_client_session::on_resolve,
                                shared_from_this()));
            }

            void
            on_resolve(
                    network::beast::error_code ec,
                    network::tcp::resolver::results_type results) {
                if (ec)
                    return network::fail(ec, "resolve");

                // Set a timeout on the operation
                stream_.expires_after(std::chrono::seconds(30));

                // Make the connection on the IP address we get from a lookup
                stream_.async_connect(
                        results,
                        network::beast::bind_front_handler(
                                &http_client_session::on_connect,
                                shared_from_this()));
            }

            void
            on_connect(network::beast::error_code ec, network::tcp::resolver::results_type::endpoint_type) {
                if (ec)
                    return network::fail(ec, "connect");

                // Set a timeout on the operation
                stream_.expires_after(std::chrono::seconds(30));

                // Send the HTTP request to the remote host
                http::async_write(stream_, req_,
                                  network::beast::bind_front_handler(
                                          &http_client_session::on_write,
                                          shared_from_this()));
            }

            void
            on_write(
                    network::beast::error_code ec,
                    std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);

                if (ec)
                    return network::fail(ec, "write");

                // Receive the HTTP response
                http::async_read(stream_, buffer_, res_,
                                 network::beast::bind_front_handler(
                                         &http_client_session::on_read,
                                         shared_from_this()));
            }

            void
            on_read(
                    network::beast::error_code ec,
                    std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);

                if (ec)
                    return network::fail(ec, "read");

                // Write the message to standard out
                std::cout << res_ << std::endl;

                // Gracefully close the socket
                stream_.socket().shutdown(network::tcp::socket::shutdown_both, ec);

                // not_connected happens sometimes so don't bother reporting it.
                if (ec && ec != network::beast::errc::not_connected)
                    return network::fail(ec, "shutdown");

                // If we get here then the connection is closed gracefully
            }
        };
}}