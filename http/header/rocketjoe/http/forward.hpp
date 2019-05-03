#pragma once

#include <boost/beast/http/verb.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

namespace rocketjoe { namespace http {
        using boost::string_view;
        namespace http = boost::beast::http;
        using http_method = http::verb;
        using request_type = http::request<http::string_body>;
        using response_type = http::response<http::string_body>;
        
        using tcp = boost::asio::ip::tcp;               // from <boost/asio/ip/tcp.hpp>
        namespace websocket = boost::beast::websocket;  // from <boost/beast/websocket.hpp>

        /**
         * template<
                    class Body,
                    class Allocator
            >
            using request = http::request<Body, http::basic_fields<Allocator>>;
         * */
}}