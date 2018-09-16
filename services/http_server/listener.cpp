#include <rocketjoe/services/http_server/listener.hpp>

namespace rocketjoe { namespace services { namespace http_server {

            listener::listener(boost::asio::io_context &ioc, tcp::endpoint endpoint, goblin_engineer::pipe *pipe_) :
                    acceptor_(ioc),
                    socket_(ioc),
                    pipe_(pipe_) {
                boost::system::error_code ec;

                // Open the acceptor
                acceptor_.open(endpoint.protocol(), ec);
                if (ec) {
                    fail(ec, "open");
                    return;
                }

                // Allow address reuse
                acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
                if (ec) {
                    fail(ec, "set_option");
                    return;
                }

                // Bind to the server address
                acceptor_.bind(endpoint, ec);
                if (ec) {
                    fail(ec, "bind");
                    return;
                }

                // Start listening for connections
                acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
                if (ec) {
                    fail(ec, "listen");
                    return;
                }
            }

            void listener::run() {
                if (!acceptor_.is_open()) {
                    return;
                }

                do_accept();
            }

            void listener::do_accept() {
                acceptor_.async_accept(
                        socket_,
                        std::bind(
                                &listener::on_accept,
                                shared_from_this(),
                                std::placeholders::_1));
            }

            void listener::on_accept(boost::system::error_code ec) {
                if (ec) {
                    fail(ec, "accept");
                } else {
                    api::transport_id id_= static_cast<api::transport_id>(std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count());
                    auto session = std::make_shared<http_session>(std::move(socket_),id_,pipe_);
                    storage_session.emplace(id_,std::move(session));
                    storage_session.at(id_)->run();
                }

                // Accept another connection
                do_accept();
            }

            void listener::write(std::unique_ptr<api::transport_base> ptr) {
                std::cerr << "id = " << ptr->id() <<std::endl;

                auto& session = storage_session.at(ptr->id());

                session->write(std::move(ptr));
            }
        }}}