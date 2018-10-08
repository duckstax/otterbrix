#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>

#include <goblin-engineer/abstract_service.hpp>

#include <rocketjoe/services/http_server/http_session.hpp>
#include <rocketjoe/services/http_server/http_context.hpp>

#include <rocketjoe/api/transport_base.hpp>

namespace rocketjoe { namespace services { namespace http_server {



            class listener final :
                    public std::enable_shared_from_this<listener>,
                    public http_context {
            public:
                listener(
                        boost::asio::io_context &ioc,
                        tcp::endpoint endpoint,
                        goblin_engineer::pipe* pipe_
                );

                ~listener() = default;

                void write(std::unique_ptr<api::transport_base>);

                void add_trusted_url(std::string name);

                auto check_url(const std::string &) const  -> bool override;

                auto send(goblin_engineer::message&&) const -> bool override;

                void run();

                void do_accept();

                void on_accept(boost::system::error_code ec);

            private:
                tcp::acceptor acceptor_;
                tcp::socket socket_;
                goblin_engineer::pipe* pipe_;
                std::unordered_set<std::string> trusted_url;
                std::unordered_map<api::transport_id,std::shared_ptr<http_session>> storage_session;


            };
        }
    }
}