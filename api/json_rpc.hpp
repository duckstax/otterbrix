#pragma once

#include <string>
#include <vector>

namespace network { namespace protocol { namespace json_rpc {

    constexpr int parse_error      = -32700;
    constexpr int invalid_request  = -32600;
    constexpr int method_not_found = -32601;
    constexpr int invalid_params   = -32602;
    constexpr int internal_error   = -32603;

    enum class json_rpc_status : char {
        request,
        response,
        response_error,

    };

    struct message_header {
        std::string jsonrpc;
        std::string method;
        std::size_t id;
        json_rpc_status status;
        virtual ~message_header() = default;
    };


    struct error {
        std::int8_t code;
        std::string message;
        //data
    };


    struct message final : public message_header {

        virtual ~message() = default;
    };


    using batch = std::vector<message>;
/*
    auto parser(const std::string&,message&) -> bool;

    auto parser(const std::string&,batch&) -> bool;
    */

}}}