#include <utility>

#include <rocketjoe/data_provider/ws/ws_server.hpp>
#include <rocketjoe/data_provider/ws/ws_listener.hpp>
#include <goblin-engineer/context.hpp>
#include <goblin-engineer/metadata.hpp>
#include <memory>
#include <rocketjoe/api/websocket.hpp>
#include <rocketjoe/data_provider/ws/ws_server.hpp>


namespace rocketjoe { namespace data_provider { namespace ws_server {

            class ws_server::impl final {
            public:
                impl() = default;

                ~impl() = default;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

                impl(goblin_engineer::dynamic_config& configuration,actor_zeta::actor::actor_address address):address__(std::move(address)) {
                    address_ = boost::asio::ip::make_address("127.0.0.1");
                    auto string_port = configuration.as_object()["http-port"].as_string();
                    auto tmp_port = std::stoul(string_port);
                    port = static_cast<unsigned short>(tmp_port);
                }

                boost::asio::ip::address address_;
                unsigned short port;
                actor_zeta::actor::actor_address address__;
                std::shared_ptr<ws_listener> listener_;


            };

            ws_server::ws_server(goblin_engineer::dynamic_config& configuration,actor_zeta::environment::abstract_environment * env,actor_zeta::actor::actor_address address):
                data_provider(env,"ws"),
                pimpl(new impl(configuration, std::move(address))) {


                    attach(
                        actor_zeta::behavior::make_handler(
                                "write",
                                [this](actor_zeta::behavior::context&,api::transport&t) -> void {
                                    pimpl->listener_->write(std::unique_ptr<api::web_socket>(
                                            api::unpack_transport<api::web_socket>(t)));
                                }
                        )
                    );


            }

            void ws_server::startup(goblin_engineer::context_t *ctx) {
                pimpl->listener_ = std::make_shared<ws_listener>(ctx->main_loop(), tcp::endpoint{pimpl->address_, pimpl->port},pimpl->address__);
                pimpl->listener_->run();

            }

            void ws_server::shutdown() {

            }

        }}}