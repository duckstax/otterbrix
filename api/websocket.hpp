#pragma once

#include "transport_base.hpp"
#include <unordered_map>
#include <string>

namespace rocketjoe { namespace api {

struct web_socket final  : public  transport_base {
    explicit web_socket(transport_id);

    ~web_socket() override = default;
    std::string body;

};

}}