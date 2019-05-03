#pragma once

#include <string>
#include <goblin-engineer/forward.hpp>
#include "forward.hpp"
#include <rocketjoe/http/transport_base.hpp>

namespace rocketjoe { namespace http {

struct http_context {

    virtual auto check_url(const std::string &) const -> bool = 0;
    virtual auto operator()( request_type&& ,api::transport_id ) const -> void = 0;
    virtual ~http_context()= default;

};

}}