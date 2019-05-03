#pragma once

#include <string>
#include <rocketjoe/http/transport_base.hpp>
#include "../../dto/header/rocketjoe/dto/json_rpc.hpp"

namespace rocketjoe { namespace api {

        struct app_info final {

            app_info() = default;
            ~app_info() = default;

            app_info(
                    const std::string &name,
                    const std::string &application_id,
                    const std::string &api_key):
                    name(name),
                    application_id(application_id),
                    api_key(api_key) {

            }
            std::string name;
            std::string application_id;
            std::string api_key;
        };

        struct task final {
            task() = default;
            ~task() = default;
            transport transport_;
            json_rpc::request_message request;
            app_info app_info_;

        };



}}