#include <utility>
#include <unordered_map>
#include <unordered_set>

#include <goblin-engineer/context.hpp>
#include <iostream>

#include "rocketjoe/services/http_server/context.hpp"
#include "rocketjoe/services/http_server/server.hpp"
#include "rocketjoe/services/http_server/session.hpp"


namespace rocketjoe { namespace http {

        using clock = std::chrono::steady_clock;

        constexpr const char *dispatcher = "dispatcher";

        class server::impl final :
                public std::enable_shared_from_this<impl>,
                public context {
        public:
            impl() = default;

            ~impl() override = default;

            impl &operator=(impl &&) = default;

            impl(impl &&) = default;

            impl &operator=(const impl &) = default;

            impl(const impl &) = default;

            impl(goblin_engineer::dynamic_config &configuration, actor_zeta::actor::actor_address address,
                 actor_zeta::actor::actor_address http_address) :
                    pipe_(std::move(address)),
                    http_address_(std::move(http_address)) {
                boost::asio::ip::address address_ = boost::asio::ip::make_address("127.0.0.1");
                auto string_port = configuration.as_object()["http-port"].as_string();
                auto tmp_port = std::stoul(string_port);
                auto port = static_cast<unsigned short>(tmp_port);
                beast::error_code ec;

                // Open the acceptor
                acceptor_.open(endpoint.protocol(), ec);

                if(ec){
                    fail(ec, "open");
                    return;
                }

                // Allow address reuse
                acceptor_.set_option(net::socket_base::reuse_address(true), ec);

                if(ec){
                    fail(ec, "set_option");
                    return;
                }

                // Bind to the server address
                acceptor_.bind(endpoint, ec);

                if(ec){
                    fail(ec, "bind");
                    return;
                }

                // Start listening for connections
                acceptor_.listen(net::socket_base::max_listen_connections, ec);
                if(ec){
                    fail(ec, "listen");
                    return;
                }
            }

            auto init(boost::asio::io_context &ioc) {
                acceptor_ = std::make_unique<tcp::acceptor>(ioc);

                boost::system::error_code ec;

                // Open the acceptor
                acceptor_->open(endpoint_.protocol(), ec);
                if (ec) {
                    fail(ec, "open");
                    return;
                }

                // Allow address reuse
                acceptor_->set_option(boost::asio::socket_base::reuse_address(true), ec);
                if (ec) {
                    fail(ec, "set_option");
                    return;
                }

                // Bind to the server address
                acceptor_->bind(endpoint_, ec);
                if (ec) {
                    fail(ec, "bind");
                    return;
                }

                // Start listening for connections
                acceptor_->listen(boost::asio::socket_base::max_listen_connections, ec);
                if (ec) {
                    fail(ec, "listen");
                    return;
                }
            }

            void run() {
                if (!acceptor_->is_open()) {
                    return;
                }

                do_accept();
            }

            void do_accept() {
                id_ = static_cast<std::size_t>(std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count());
                // The new connection gets its own strand
                acceptor_.async_accept(
                        net::make_strand(ioc_),
                        beast::bind_front_handler(
                                &impl::on_accept,
                                shared_from_this()));
            }

            void on_accept(boost::system::error_code ec) {
                if (ec) {
                    fail(ec, "accept");
                } else {
                    storage_session.emplace(id_, std::shared_ptr<session>(session_.release()));
                    storage_session.at(id_)->run();
                }

                // Accept another connection
                do_accept();
            }

            void write(response_context_type &body) {
                std::cerr << "id = " << body.id() << std::endl;

                auto &session = storage_session.at(body.id());

                session->write(std::move(body.response()));
            }

            void add_trusted_url(std::string name) {
                trusted_url.emplace(std::move(name));
            }

        protected:
            auto check_url(const std::string &url) const -> bool override {
                ///TODO: not fast
                auto start = url.begin();
                ++start;
                return (trusted_url.find(std::string(start, url.end())) != trusted_url.end());
            }

            auto operator()(request_type &&req, std::size_t session_id) const -> void override {

                query_context context(std::move(req), session_id, http_address_);

                pipe_->send(
                        actor_zeta::messaging::make_message(
                                http_address_,
                                dispatcher,
                                std::move(context)
                        )
                );

            }

        private:
            net::io_context& ioc_;
            tcp::acceptor acceptor_;
            std::size_t id_;
            actor_zeta::actor::actor_address pipe_;
            actor_zeta::actor::actor_address http_address_;
            std::unordered_set<std::string> trusted_url;
            std::unordered_map<std::size_t, std::shared_ptr<session>> storage_session;

        };

        server::server(
                goblin_engineer::dynamic_config &configuration,
                actor_zeta::environment::abstract_environment *env,
                actor_zeta::actor::actor_address address
        ) :
                data_provider(env, "http"),
                pimpl(std::make_shared<impl>(configuration, std::move(address), std::move(self()))) {


            attach(
                    actor_zeta::behavior::make_handler(
                            "write",
                            [this](actor_zeta::behavior::context &ctx, response_context_type &body) -> void {
                                pimpl->write(body);
                            }
                    )
            );

            attach(
                    actor_zeta::behavior::make_handler(
                            "add_trusted_url",
                            [this](actor_zeta::behavior::context &ctx) -> void {
                                auto app_name = ctx.message().body<std::string>();
                                pimpl->add_trusted_url(app_name);
                            }
                    )
            );

        }

        void server::startup(goblin_engineer::context_t *ctx) {
            pimpl->init(ctx->main_loop());
            pimpl->run();
        }

        void server::shutdown() {

        }

        void fail(boost::system::error_code ec, char const *what) {
            if(ec == net::ssl::error::stream_truncated)
                return;

            std::cerr << what << ": " << ec.message() << "\n";
        }
}}