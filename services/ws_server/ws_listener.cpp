#include <rocketjoe/services/ws_server/ws_listener.hpp>
#include <rocketjoe/api/websocket.hpp>

namespace RocketJoe { namespace services { namespace ws_server {

            ws_listener::ws_listener(boost::asio::io_context &ioc, tcp::endpoint endpoint, goblin_engineer::pipe *pipe_) :
                    strand_(ioc.get_executor()),
                    acceptor_(ioc),
                    socket_(ioc),
                    pipe_(pipe_){

                boost::system::error_code ec;

                acceptor_.open(endpoint.protocol(), ec);
                if (ec) {
                    fail(ec, "open");
                    return;
                }

                acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);

                if (ec) {
                    fail(ec, "set_option");
                    return;
                }

                acceptor_.bind(endpoint, ec);
                if (ec) {
                    fail(ec, "bind");
                    return;
                }

                acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);

                if (ec) {
                    fail(ec, "listen");
                    return;
                }
            }

            void ws_listener::run() {
                if (!acceptor_.is_open())
                    return;
                do_accept();
            }

            void ws_listener::do_accept() {
                acceptor_.async_accept(
                        socket_,
                        boost::asio::bind_executor(
                                strand_,
                                std::bind(
                                        &ws_listener::on_accept,
                                        shared_from_this(),
                                        std::placeholders::_1)));
            }

            void ws_listener::on_accept(boost::system::error_code ec) {

                if (ec) {
                    fail(ec, "accept");
                } else {
                    transport::transport_id id_= static_cast<transport::transport_id>(std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count());
                    auto session = std::make_shared<ws_session>(std::move(socket_),id_,pipe_);
                    storage_sessions.emplace(id_,std::move(session));
                    storage_sessions.at(id_)->run();

                }

                do_accept();
            }

            void ws_listener::write(std::unique_ptr<transport::web_socket>ptr) {
                storage_sessions.at(ptr->id())->write(std::move(ptr));
            }

        }}}