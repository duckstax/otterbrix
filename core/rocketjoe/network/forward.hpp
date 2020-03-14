#pragma once

#include <functional>
#include <rocketjoe/network/network.hpp>

namespace rocketjoe { namespace network {

    using helper_write_f_t = std::function<void(network::request_type && , std::size_t)>;
    using boost::string_view;
    ///using rocketjoe::http::http_method;
    ///using rocketjoe::http::options;
    ///using rocketjoe::http::query_context;

}}