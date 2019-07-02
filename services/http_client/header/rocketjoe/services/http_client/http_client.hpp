#pragma once

#include <memory>
#include <chrono>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/strand.hpp>
#include <iostream>

#include "rocketjoe/http/http.hpp"

namespace rocketjoe { namespace http {
        namespace beast = boost::beast;         // from <boost/beast.hpp>
        namespace http = beast::http;           // from <boost/beast/http.hpp>
        namespace net = boost::asio;            // from <boost/asio.hpp>
        using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>

        class http_client_session final : public std::enable_shared_from_this<http_client_session> {
            tcp::resolver resolver_;
            beast::tcp_stream stream_;
            beast::flat_buffer buffer_; // (Must persist between reads)
            http::request<http::string_body> req_;
            http::response<http::string_body> res_;

        public:
            // Objects are constructed with a strand to
            // ensure that handlers do not execute concurrently.
            explicit
            http_client_session(net::io_context& ioc):

            resolver_ (net::make_strand(ioc))
            , stream_(net::make_strand(ioc)) {
            }

            // Start the asynchronous operation
            void
            run(
                    char const *host,
                    char const *port,
                    char const *target) {

                req_.version(11);
                req_.method(http::verb::get);
                req_.target(target);
                req_.set(http::field::host, host);

                // Look up the domain name
                resolver_.async_resolve(
                        host,
                        port,
                        beast::bind_front_handler(
                                &http_client_session::on_resolve,
                                shared_from_this()));
            }

            void
            on_resolve(
                    beast::error_code ec,
                    tcp::resolver::results_type results) {
                if (ec)
                    return fail(ec, "resolve");

                // Set a timeout on the operation
                stream_.expires_after(std::chrono::seconds(30));

                // Make the connection on the IP address we get from a lookup
                stream_.async_connect(
                        results,
                        beast::bind_front_handler(
                                &http_client_session::on_connect,
                                shared_from_this()));
            }

            void
            on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
                if (ec)
                    return fail(ec, "connect");

                // Set a timeout on the operation
                stream_.expires_after(std::chrono::seconds(30));

                // Send the HTTP request to the remote host
                http::async_write(stream_, req_,
                                  beast::bind_front_handler(
                                          &http_client_session::on_write,
                                          shared_from_this()));
            }

            void
            on_write(
                    beast::error_code ec,
                    std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);

                if (ec)
                    return fail(ec, "write");

                // Receive the HTTP response
                http::async_read(stream_, buffer_, res_,
                                 beast::bind_front_handler(
                                         &http_client_session::on_read,
                                         shared_from_this()));
            }

            void
            on_read(
                    beast::error_code ec,
                    std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);

                if (ec)
                    return fail(ec, "read");

                // Write the message to standard out
                std::cout << res_ << std::endl;

                // Gracefully close the socket
                stream_.socket().shutdown(tcp::socket::shutdown_both, ec);

                // not_connected happens sometimes so don't bother reporting it.
                if (ec && ec != beast::errc::not_connected)
                    return fail(ec, "shutdown");

                // If we get here then the connection is closed gracefully
            }
        };
}}