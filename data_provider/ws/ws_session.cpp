#include <rocketjoe/data_provider/ws/ws_session.hpp>
#include <rocketjoe/api/websocket.hpp>

namespace rocketjoe { namespace data_provider { namespace ws {

    constexpr const  char* dispatcher = "dispatcher";

            void ws_session::on_write(boost::system::error_code ec, std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);

                if (ec) {
                    return fail(ec, "write");
                }
                // Clear the buffer
                buffer_.consume(buffer_.size());

                // Do another read
                do_read();
            }

            void ws_session::on_read(boost::system::error_code ec, std::size_t bytes_transferred) {
                boost::ignore_unused(bytes_transferred);

                if(ec == websocket::error::closed) {
                    return;
                }

                if(ec) {
                    fail(ec, "read");
                }

                auto* ws = new api::web_socket (id_);
                ws->body = boost::beast::buffers_to_string(buffer_.data());
                api::transport ws_data(ws);
                pipe_->send(
                        actor_zeta::messaging::make_message(
                                pipe_,
                                dispatcher,
                                std::move(ws_data)
                        )
                );
                ws_.text(ws_.got_text());
                ws_.async_write(
                        buffer_.data(),
                        boost::asio::bind_executor(
                                strand_,
                                std::bind(
                                        &ws_session::on_write,
                                        shared_from_this(),
                                        std::placeholders::_1,
                                        std::placeholders::_2
                                )
                        )
                );
            }

            void ws_session::do_read() {
                ws_.async_read(
                        buffer_,
                        boost::asio::bind_executor(
                                strand_,
                                std::bind(
                                        &ws_session::on_read,
                                        shared_from_this(),
                                        std::placeholders::_1,
                                        std::placeholders::_2
                                )
                        )
                );
            }

            void ws_session::on_accept(boost::system::error_code ec) {
                if(ec) {
                    return fail(ec, "accept");
                }

                do_read();
            }

            void ws_session::run() {
                ws_.async_accept_ex(
                        [](websocket::response_type& res) {

                        },
                        boost::asio::bind_executor(
                                strand_,
                                std::bind(
                                        &ws_session::on_accept,
                                        shared_from_this(),
                                        std::placeholders::_1
                                )
                        )
                );
            }

            ws_session::ws_session(tcp::socket socket,std::size_t id, actor_zeta::actor::actor_address pipe_) :
                    ws_(std::move(socket)),
                    strand_(ws_.get_executor()),
                    id_(id),
                    pipe_(std::move(pipe_)){
                setup_stream(ws_);
            }

            void ws_session::write(std::unique_ptr<api::transport_base>) {

            }


        }}}