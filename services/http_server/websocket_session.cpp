#include "rocketjoe/services/http_server/websocket_session.hpp"

namespace rocketjoe { namespace http {

        websocket_session::websocket_session(tcp::socket &&socket, context &pipe_) :
                ws_(std::move(socket)),
                pipe_(pipe_) {
        }

        void websocket_session::on_accept(beast::error_code ec) {
            if (ec)
                return fail(ec, "accept");

            // Read a message
            do_read();
        }


        void websocket_session::on_read(
                beast::error_code ec,
                std::size_t bytes_transferred) {
            boost::ignore_unused(bytes_transferred);

            // This indicates that the websocket_session was closed
            if (ec == websocket::error::closed)
                return;

            if (ec)
                fail(ec, "read");

            // Echo the message
            ws_.text(ws_.got_text());
            ws_.async_write(
                    buffer_.data(),
                    beast::bind_front_handler(
                            &websocket_session::on_write,
                            shared_from_this()));
        }

        void websocket_session::on_write(
                beast::error_code ec,
                std::size_t bytes_transferred) {
            boost::ignore_unused(bytes_transferred);

            if (ec)
                return fail(ec, "write");

            // Clear the buffer
            buffer_.consume(buffer_.size());

            // Do another read
            do_read();
        }
}}