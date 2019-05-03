#include <rocketjoe/http/listener.hpp>
#include <chrono>
#include <rocketjoe/http/router.hpp>

namespace rocketjoe { namespace http {

    using clock = std::chrono::steady_clock;

    constexpr const char *dispatcher = "dispatcher";

    listener::listener(boost::asio::io_context &ioc, tcp::endpoint endpoint, actor_zeta::actor::actor_address pipe_,actor_zeta::actor::actor_address self) :
            acceptor_(ioc),
            socket_(ioc),
            pipe_(std::move(pipe_)),
            self(self){
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
                    auto id_= static_cast<std::size_t>(std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count());
                    auto session = std::make_shared<http_session>(std::move(socket_),id_,*this);
                    storage_session.emplace(id_,std::move(session));
                    storage_session.at(id_)->run();
                }

                // Accept another connection
                do_accept();
            }

            void listener::write(response_context_type&body) {
                std::cerr << "id = " << body.id() <<std::endl;

                auto &session = storage_session.at(body.id());

                session->write(std::move(body.response()));
            }

            void listener::add_trusted_url(std::string name) {
                trusted_url.emplace(std::move(name));
            }

            auto listener::check_url(const std::string &url) const -> bool {
                ///TODO: not fast
                auto start = url.begin();
                ++start;
                return (trusted_url.find(std::string(start,url.end()))!=trusted_url.end());
            }

            auto listener::operator()(request_type && req,std::size_t session_id) const -> void {

                http_query_context  context(std::move(req),session_id,std::move(self));

                pipe_->send(
                        actor_zeta::messaging::make_message(
                                pipe_,
                                dispatcher,
                                std::move(context)
                        )
                );

            }


}}