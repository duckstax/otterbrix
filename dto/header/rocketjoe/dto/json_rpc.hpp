#pragma once

#include <string>
#include <vector>
#include <boost/optional.hpp>
#include <iostream>
#include "nlohmann/json.hpp"

namespace rocketjoe { namespace api { namespace json_rpc {

            using nlohmann::json;

            enum class type : char {
                non,
                request,
                response,
                error,
                notify
            };

            enum class error_code {
                parse_error = -32700,
                invalid_request = -32600,
                methodNot_found = -32601,
                invalid_params = -32602,
                internal_error = -32603,
                server_error_Start = -32099,
                server_error_end = -32000,
                server_not_initialized,
                unknown_error_code = -32001
            };

            struct request_message final {
                request_message() = default;

                ~request_message() = default;

                request_message(
                        std::string id,
                        std::string method,
                        json params) :
                        id(std::move(id)),
                        method(std::move(method)),
                        params(std::move(params)) {}

                request_message(
                        uint64_t id,
                        std::string method,
                        json params = json()) :
                        id(id),
                        method(std::move(method)),
                        params(std::move(params)) {

                }

                json id;
                std::string method;
                json params;
                boost::optional<std::string> application_id;
                boost::optional<std::string> api_key;
            };


            struct response_error final {
                response_error() = default;

                ~response_error() = default;

                error_code code;
                std::string message;
                json data;

                response_error(error_code code, std::string message)
                        : code(code), message(std::move(message)) {}

            };

            struct response_message final {
                response_message() = default;

                ~response_message() = default;

                response_message(std::string id, json result = json())
                        : id(std::move(id)), result(std::move(result)) {}

                response_message(int id, json result = json())
                        : id(uint64_t(id)), result(std::move(result)) {}

                json id;
                json result;
                boost::optional<response_error> error;
            };

            struct notify_message final {
                notify_message(std::string method, json params = json()) :
                        method(std::move(method)),
                        params(std::move(params)) {}

                std::string method;
                json params;
            };

///Experimental }
            class context final {
            public:
                context() : type_(type::non) {}

                ~context() = default;

                void make_request(
                        std::string id,
                        std::string method,
                        json params) {
                    assert( type_ != type::non );
                    type_ = type::request;
                    this->id = std::move(id);
                    method_ = std::move(method);
                    this->params = std::move(params);
                }

                void make_request(uint64_t id, std::string method, json params = json()) {
                    assert( type_ != type::non );
                    type_ = type::request;
                    this->id = id;
                    method_ = std::move(method);
                    this->params = std::move(params);

                }

                void make_notify(std::string method, json params = json()) {
                    assert(type_!=type::non);
                    type_ = type::notify;
                    method_ = std::move(method);
                    this->params = std::move(params);
                }

                void make_response(std::string id, json result = json()) {
                    assert(type_!=type::non);
                    type_ = type::response;
                    this->id = std::move(id);
                    this->result = std::move(result);
                }

                void make_response(uint64_t id, json result = json()) {
                    assert(type_!=type::non);
                    type_ = type::response;
                    this->id = uint64_t(id);
                    this->result = std::move(result);
                }

                void make_error(error_code code, std::string message) {
                    assert(type_!=type::non);
                    this->code = code;
                    this->message = std::move(message);
                }

                const std::string& method() const {
                    return method_;
                }


            private:
                type type_;
                json id;
                std::string method_;
                json params;
                json result;
                error_code code;
                std::string message;
                json data;
            };

            ///Experimental }

            bool parse(const std::string &raw, request_message &request);

            bool parse(const json &message, notify_message &notify);

            bool parse(const json &message, response_message &response);

            std::string serialize(const request_message &msg);

            std::string serialize(const response_message &msg);

            std::string serialize(const notify_message &msg);

}}}