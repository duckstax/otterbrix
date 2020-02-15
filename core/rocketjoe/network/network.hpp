#pragma once

#include <sstream>
#include <unordered_map>
#include <string>
#include <functional>

#include <boost/asio.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <boost/beast/core.hpp>

#include <boost/beast/http.hpp>
#include <boost/beast/http/verb.hpp>

#include <boost/beast/websocket.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <actor-zeta/core.hpp>

namespace rocketjoe { namespace network {
        using boost::string_view;

        namespace net = boost::asio;
        using net::ip::tcp;
        
        namespace beast = boost::beast;
        namespace http = beast::http;
        using http_method = http::verb;
        using request_type = http::request<http::string_body>;
        using response_type = http::response<http::string_body>;

        namespace websocket = boost::beast::websocket;

        void fail(boost::system::error_code ec, char const* what);

        /**
         * template<
                    class Body,
                    class Allocator
            >
            using request = http::request<Body, http::basic_fields<Allocator>>;
         * */

        class options final {
        public:
            constexpr options() = default;

            ~options() = default;
        };


        class response_context_type final {
        public:
            response_context_type() = default;

            response_context_type(const response_context_type &) = default; // todo hack
            response_context_type &operator=(const response_context_type &) = delete;

            response_context_type(response_context_type &&) = default;

            response_context_type &operator=(response_context_type &&) = default;

            ~response_context_type() = default;

            response_context_type(
                    response_type response_,
                    std::size_t i
            ) :
                    response_(std::move(response_)),
                    id_(i) {

            }

            response_context_type(
                    std::size_t i
            ) :
                    id_(i) {

            }


            auto response() -> response_type & {
                return response_;
            }

            auto id() {
                return id_;
            }

        private:
            response_type response_;
            std::size_t id_;

        };


        class query_context final {
        public:
            query_context() = default;

            query_context(const query_context &) = default; // todo hack
            query_context &operator=(const query_context &) = delete;

            query_context(query_context &&) = default;

            query_context &operator=(query_context &&) = default;

            ~query_context() = default;

            query_context(
                    request_type request_,
                    size_t i,
                    actor_zeta::actor::actor_address address
            ) :
                    request_(std::move(request_)),
                    id_(i),
                    address(std::move(address)) {

            }

            auto request() -> request_type & {
                return request_;
            }

            auto response() -> response_type & {
                return response_;
            }

            auto id() {
                return id_;
            }

            /*
            auto response(response_type&&response_) {
                this->response_ = std::move(response_);
            }
            */
            auto write() {
                response_.prepare_payload();
                response_context_type context(std::move(response_), id_);
                actor_zeta::send(
                        address,
                        actor_zeta::messaging::make_message(
                                address,
                                "write",
                                std::move(context)

                        )
                );
            }

        private:
            request_type request_;
            std::size_t id_;
            actor_zeta::actor::actor_address address;
            response_type response_;
        };

    }}