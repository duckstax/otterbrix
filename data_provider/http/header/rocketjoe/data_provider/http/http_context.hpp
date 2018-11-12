#pragma once

#include <string>

#include <rocketjoe/api/transport_base.hpp>
#include <goblin-engineer/forward.hpp>
#include <boost/beast/http/string_body.hpp>

namespace rocketjoe { namespace data_provider { namespace http_server {
            namespace http = boost::beast::http;
struct http_context {

    virtual auto check_url(const std::string &) const -> bool = 0;
    virtual auto operator()( http::request <http::string_body>&& ,api::transport_id ) const -> void = 0;
    virtual ~http_context()= default;

};

}}}