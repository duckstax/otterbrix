#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>

#include <rocketjoe/http/http_session.hpp>
#include <rocketjoe/http/http_context.hpp>
#include <goblin-engineer/abstract_service.hpp>
#include "router.hpp"

namespace rocketjoe { namespace http {

            class listener final :
                    public std::enable_shared_from_this<listener>,
                    public http_context {
            public:
                listener(
                        boost::asio::io_context &ioc,
                        tcp::endpoint endpoint,
                        actor_zeta::actor::actor_address pipe_,
                        actor_zeta::actor::actor_address self
                );

                ~listener() override = default;

                void write(response_context_type&);

                void add_trusted_url(std::string name);

                auto check_url(const std::string &) const  -> bool override;

                auto operator()(http::request <http::string_body>&& ,std::size_t) const -> void override;

                void run();

                void do_accept();

                void on_accept(boost::system::error_code ec);

            private:
                tcp::acceptor acceptor_;
                tcp::socket socket_;
                actor_zeta::actor::actor_address pipe_;
                actor_zeta::actor::actor_address self;
                std::unordered_set<std::string> trusted_url;
                std::unordered_map<std::size_t,std::shared_ptr<http_session>> storage_session;


            };
}}