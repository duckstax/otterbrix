#include <rocketjoe/services/http_server/http_server.hpp>
#include <goblin-engineer/context.hpp>
#include <goblin-engineer/metadata.hpp>

#include <rocketjoe/services/http_server/listener.hpp>


namespace rocketjoe { namespace services { namespace http_server {

            class http_server::impl final {
            public:
                impl() = default;

                ~impl() = default;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

                std::shared_ptr<listener> listener_;

            };

            http_server::http_server(goblin_engineer::context_t *ctx) : pimpl(std::make_unique<impl>()) {
                auto& config = ctx->config();

                boost::asio::ip::address address =  boost::asio::ip::make_address(config.as_object()["address"].as_string());
                auto string_port = config.as_object()["http-port"].as_string();
                auto tmp_port = std::stoul(string_port);
                auto port = static_cast<unsigned short>(tmp_port);

                pimpl->listener_ = std::make_shared<listener>(ctx->main_loop(), tcp::endpoint{address, port},to_pipe());

                add(
                        "write",
                        [this](goblin_engineer::message&& message) -> void {
                            auto arg = message.args[0];
                            auto t = boost::any_cast<api::transport>(arg);
                            auto*transport_tmp = t.transport_.get();
                            t.transport_.reset();
                            std::unique_ptr<api::http> transport(static_cast<api::http*>(transport_tmp));
                            pimpl->listener_->write(std::move(transport));
                        }
                );

            }

            http_server::~http_server() {

            }

            void http_server::metadata(goblin_engineer::metadata_service *metadata) const {
                metadata->name = "http";
            }

            void http_server::startup(goblin_engineer::context_t *ctx) {
                pimpl->listener_->run();
            }

            void http_server::shutdown() {

            }

            std::string http_server::name() const {
                return std::string("http");
            }
        }
    }
}