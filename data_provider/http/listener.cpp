#include <rocketjoe/data_provider/http/listener.hpp>
#include <chrono>

namespace rocketjoe { namespace data_provider { namespace http {

            using clock = std::chrono::steady_clock;

            constexpr const char *dispatcher = "dispatcher";

            listener::listener(boost::asio::io_context &ioc, tcp::endpoint endpoint, actor_zeta::actor::actor_address pipe_) :
                    acceptor_(ioc),
                    socket_(ioc),
                    pipe_(std::move(pipe_)) {
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
                    auto id_= static_cast<api::transport_id>(std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count());
                    auto session = std::make_shared<http_session>(std::move(socket_),id_,*this);
                    storage_session.emplace(id_,std::move(session));
                    storage_session.at(id_)->run();
                }

                // Accept another connection
                do_accept();
            }

            void listener::write(std::unique_ptr<api::transport_base> ptr) {
                std::cerr << "id = " << ptr->id() <<std::endl;

                auto &session = storage_session.at(ptr->id());

                session->write(std::move(ptr));
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

            auto listener::operator()(http::request <http::string_body>&& req,api::transport_id id) const -> void {

                auto* http = new api::http(id);

                http->method(std::string(req.method_string()));

                http->uri(std::string(req.target()));

                for (auto const &field : req) {

                    std::string header_name(field.name_string());
                    std::string header_value(field.value());
                    http->header(std::move(header_name), std::move(header_value));
                }

                http->body(req.body());

                //api::transport http_data(std::move(http_transport_));
                pipe_->send(
                        actor_zeta::messaging::make_message(
                                pipe_,
                                dispatcher,
                                api::transport(http)
                        )
                );

            }


}}}