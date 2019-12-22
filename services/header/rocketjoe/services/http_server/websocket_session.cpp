#include "rocketjoe/services/http_server/websocket_session.hpp"


namespace rocketjoe { namespace session {

        websocket_session::websocket_session(network::tcp::socket &&socket, network::helper_write_f_t pipe_) :
                ws_(std::move(socket)),
                pipe_(pipe_) {
        }

        void websocket_session::on_accept(network::beast::error_code ec) {
            if (ec)
                return network::fail(ec, "accept");

            // Read a message
            do_read();
        }


        void websocket_session::on_read(
                network::beast::error_code ec,
                std::size_t bytes_transferred) {
            boost::ignore_unused(bytes_transferred);

            // This indicates that the websocket_session was closed
            if (ec == network::websocket::error::closed)
                return;

            if (ec) {
                network::fail(ec, "read");
            }

            // Echo the message
            ws_.text(ws_.got_text());
            ws_.async_write(
                    buffer_.data(),
                    network::beast::bind_front_handler(
                            &websocket_session::on_write,
                            shared_from_this()));
        }

        void websocket_session::on_write(
                network::beast::error_code ec,
                std::size_t bytes_transferred) {
            boost::ignore_unused(bytes_transferred);

            if (ec)
                return network::fail(ec, "write");

            // Clear the buffer
            buffer_.consume(buffer_.size());

            // Do another read
            do_read();
        }

        void websocket_session::do_read() {
            ws_.async_read(
                    buffer_,
                    network::beast::bind_front_handler(
                            &websocket_session::on_read,
                            shared_from_this()));
        }


    }}