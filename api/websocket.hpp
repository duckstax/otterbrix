#pragma once

#include "transport_base.hpp"
#include <unordered_map>
#include <string>

namespace rocketjoe { namespace api {

struct web_socket final  : public  transport_base {
    web_socket(transport_id);
    virtual ~web_socket();
    std::string body;

};

}}