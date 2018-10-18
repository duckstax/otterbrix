#include <rocketjoe/services/ws_server/ws_server.hpp>
#include <rocketjoe/services/ws_server/ws_listener.hpp>
#include <goblin-engineer/context.hpp>
#include <goblin-engineer/metadata.hpp>
#include <memory>
#include <goblin-engineer/message.hpp>
#include <rocketjoe/api/websocket.hpp>

namespace rocketjoe { namespace services { namespace ws_server {

            class ws_server::impl final {
            public:
                impl() = default;

                ~impl() = default;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

                std::shared_ptr<ws_listener> listener_;

            };

            ws_server::ws_server(goblin_engineer::context_t *ctx) : pimpl(std::make_unique<impl>()) {
                auto& config = ctx->config();
                boost::asio::ip::address address =  boost::asio::ip::make_address(config.as_object()["address"].as_string());
                auto string_port = config.as_object()["ws-port"].as_string();
                auto tmp_port = std::stoul(string_port);
                auto port = static_cast<unsigned short>(tmp_port);
                pimpl->listener_ = std::make_shared<ws_listener>(ctx->main_loop(), tcp::endpoint{address, port},to_pipe());

                add(
                        "write",
                        [this](goblin_engineer::message&& message) -> void {
                            auto arg = message.args[0];
                            auto t = std::move(boost::any_cast<api::transport>(arg));
                            auto*transport_tmp = t.transport_.get();
                            t.transport_.reset();
                            std::unique_ptr<api::web_socket> transport(static_cast<api::web_socket*>(transport_tmp));
                            pimpl->listener_->write(std::move(transport));
                        }
                );


            }

            ws_server::~ws_server() = default;

            void ws_server::metadata(goblin_engineer::metadata_service *metadata) const {
                metadata->name = "ws";
            }

            void ws_server::startup(goblin_engineer::context_t *ctx) {

                pimpl->listener_->run();

            }

            void ws_server::shutdown() {

            }

            std::string ws_server::name() const {
                return "ws";
            }


        }
    }
}