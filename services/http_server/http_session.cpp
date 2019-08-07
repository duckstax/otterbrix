#include <rocketjoe/services/http_server/http_session.hpp>

#include <rocketjoe/network/network.hpp>
#include <rocketjoe/services/http_server/websocket_session.hpp>


namespace rocketjoe { namespace session {

        http_session::http_session(network::tcp::socket &&socket, std::size_t id, network::helper_write_f_t  context_) :
                stream_(std::move(socket)),
                queue_(*this),
                handle_processing(context_),
                id(id) {
        }

        void http_session::run() {
            do_read();
        }

        void http_session::on_read(network::beast::error_code ec, std::size_t bytes_transferred) {
            boost::ignore_unused(bytes_transferred);

            // This means they closed the connection
            if (ec == network::http::error::end_of_stream)
                return do_close();

            if (ec)
                return network::fail(ec, "read");

            // See if it is a WebSocket Upgrade
            if (network::websocket::is_upgrade(parser_->get())) {
                // Create a websocket session, transferring ownership
                // of both the socket and the HTTP request.
                std::make_shared<session::websocket_session>(stream_.release_socket(),handle_processing)->do_accept(parser_->release());
                return;
            }

            // Send the response
            handle_processing( std::move(parser_->release()), id);

            // If we aren't at the queue limit, try to pipeline another request
            if (!queue_.is_full())
                do_read();
        }

        void http_session::on_write(bool close, network::beast::error_code ec, std::size_t bytes_transferred) {
            boost::ignore_unused(bytes_transferred);

            if (ec)
                return network::fail(ec, "write");

            if (close) {
                // This means we should close the connection, usually because
                // the response indicated the "Connection: close" semantic.
                return do_close();
            }

            // Inform the queue that a write completed
            if (queue_.on_write()) {
                // Read another request
                do_read();
            }
        }

        void http_session::do_close() {
            network::beast::error_code ec;
            stream_.socket().shutdown(network::tcp::socket::shutdown_send, ec);
        }

        void http_session::do_read() {
            // Construct a new parser for each message
            parser_.emplace();

            // Apply a reasonable limit to the allowed size
            // of the body in bytes to prevent abuse.
            parser_->body_limit(10000);

            // Set the timeout.
            stream_.expires_after(std::chrono::seconds(30));

            // Read a request using the parser-oriented interface
            network::http::async_read(
                    stream_,
                    buffer_,
                    *parser_,
                    network::beast::bind_front_handler(
                            &http_session::on_read,
                            shared_from_this()
                    )
            );
        }

        void http_session::write(network::response_type &&body) {
            queue_(std::move(body));
        }

        http_session::queue::queue(http_session &self) : self_(self) {
            static_assert(limit > 0, "queue limit must be positive");
            items_.reserve(limit);
        }

        bool http_session::queue::on_write() {
            assert(!items_.empty());
            auto const was_full = is_full();
            items_.erase(items_.begin());
            if (!items_.empty())
                (*items_.front())();
            return was_full;
        }
    }}