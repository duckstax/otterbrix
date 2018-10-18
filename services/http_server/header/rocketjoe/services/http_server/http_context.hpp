#pragma once

#include <string>

#include <goblin-engineer/message.hpp>

namespace rocketjoe { namespace services { namespace http_server {

struct http_context {

    virtual auto check_url(const std::string &) const -> bool = 0;
    virtual auto send(goblin_engineer::message&&) const -> bool = 0;
    virtual ~http_context()= default;

};

}}}