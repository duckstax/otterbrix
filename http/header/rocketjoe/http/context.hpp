#pragma once

#include <string>
#include <goblin-engineer/forward.hpp>
#include "forward.hpp"


namespace rocketjoe { namespace http {

struct context {

    virtual auto check_url(const std::string &) const -> bool = 0;
    virtual auto operator()( request_type&& ,std::size_t session_id ) const -> void = 0;
    virtual ~context()= default;

};

}}