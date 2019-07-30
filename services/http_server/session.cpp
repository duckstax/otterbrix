#include <rocketjoe/services/http_server/session.hpp>

namespace rocketjoe { namespace http {
            using actor_zeta::messaging::message;
            constexpr const char *router = "lua_engine";

            session::session( tcp::socket&& socket, std::size_t id, context& context_):
                    stream_(std::move(socket)),
                    queue_(*this),
                    handle_processing(context_),
                    id(id) {
            }

            void session::run() {
                do_read();
            }

            void session::on_read(boost::system::error_code ec){
                boost::ignore_unused(bytes_transferred);

                // This means they closed the connection
                if(ec == http::error::end_of_stream)
                    return do_close();

                if(ec)
                    return fail(ec, "read");

                // See if it is a WebSocket Upgrade
                if(websocket::is_upgrade(parser_->get())){
                    // Create a websocket session, transferring ownership
                    // of both the socket and the HTTP request.
                    std::make_shared<websocket_session>(stream_.release_socket())->do_accept(parser_->release());
                    return;
                }

                // Send the response
                handle_processing(std::move(parser_->release()), queue_);

                // If we aren't at the queue limit, try to pipeline another request
                if(! queue_.is_full())
                    do_read();
            }

            void session::on_write(bool close, beast::error_code ec, std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);

                if(ec)
                    return fail(ec, "write");

                if(close)
                {
                    // This means we should close the connection, usually because
                    // the response indicated the "Connection: close" semantic.
                    return do_close();
                }

                // Inform the queue that a write completed
                if(queue_.on_write())
                {
                    // Read another request
                    do_read();
                }
            }

            void session::do_close() {
                // Send a TCP shutdown
                beast::error_code ec;
                stream_.socket().shutdown(tcp::socket::shutdown_send, ec);

                // At this point the connection is closed gracefully
            }

            void session::do_read() {
                // Construct a new parser for each message
                parser_.emplace();

                // Apply a reasonable limit to the allowed size
                // of the body in bytes to prevent abuse.
                parser_->body_limit(10000);

                // Set the timeout.
                stream_.expires_after(std::chrono::seconds(30));

                // Read a request using the parser-oriented interface
                http::async_read(
                        stream_,
                        buffer_,
                        *parser_,
                        beast::bind_front_handler(
                                &http_session::on_read,
                                shared_from_this()));
            }

            void session::write(response_type&&body) {
                queue_(std::move(body));
            }

        auto session::socket() -> tcp::socket & {
            return socket_;
        }


        bool session::queue::on_write() {
                assert(!items_.empty());
                auto const was_full = is_full();
                items_.erase(items_.begin());
                if (!items_.empty())
                    (*items_.front())();
                return was_full;
            }

            bool session::queue::is_full() const {
                return items_.size() >= limit;
            }

            session::queue::queue(session &self) : self_(self) {
                static_assert(limit > 0, "queue_vm limit must be positive");
                items_.reserve(limit);
            }
}}