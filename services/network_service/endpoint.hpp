#pragma once

#include <string>

using host_t = std::string;
using port_t = std::string;
using target_t = std::string;

class endpoint_t final {
public:
    host_t host_;
    port_t port_;
    target_t target_;

    endpoint_t(
        const host_t& host,
        const port_t& port,
        const target_t& target)
        : host_(host)
        , port_(port)
        , target_(target) {}

    endpoint_t(
        host_t&& host,
        port_t&& port,
        target_t&& target)
        : host_(std::move(host))
        , port_(std::move(port))
        , target_(std::move(target)) {}

    endpoint_t(endpoint_t&& endpoint) = default;
    endpoint_t(const endpoint_t& endpoint) = default;

    ~endpoint_t() = default;
};
