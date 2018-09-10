#pragma once

#include <string>
#include <vector>
#include <boost/optional.hpp>
#include "nlohmann/json.hpp"

namespace rocketjoe {
    namespace api {
        namespace json_rpc {

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
                    assert(type_!=type::non);
                    type_ = type::request;
                    this->id = std::move(id);
                    method_ = std::move(method);
                    this->params = std::move(params);
                }

                void make_request(uint64_t id, std::string method, json params = json()) {
                    assert(type_!=type::non);
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

            bool contains(const json &msg, const std::string &key) {
                return msg.find(key) != msg.end();
            }

            bool is_notify(const json &msg) {
                return msg.is_object() && !contains(msg, "id");
            }

            ////TODO FIX
            bool is_request(const json &msg) {
                return msg.is_object() /*&& contains(msg,"method")*/ && msg.at("method").is_string();
            }

            bool is_response(const json &msg) {
                return msg.is_object() && !contains(msg, "method") && contains(msg, "id");
            }

            bool parse(const json &message, request_message &request) {
                if (!is_request(message)) {
                    return false;
                }
                request.id = message.at("id");

                request.method = message.at("method").get<std::string>();
                if (contains(message, "param")) {
                    request.params = message["param"];
                } else {
                    request.params = message["params"];
                }
                return true;
            }

            bool parse(const json &message, notify_message &notify) {
                if (!is_notify(message)) {
                    return false;
                }
                notify.method = message.at("method").get<std::string>();
                if (contains(message, "param")) {
                    notify.params = message.at("param");
                } else {
                    notify.params = message.at("params");
                }
                return true;
            }

            bool parse(const json &message, response_message &response) {
                if (!is_response(message)) {
                    return false;
                }
                response.id = message.at("id");
                response.result = message.at("result");
                return true;
            }

            std::string serialize(const request_message &msg) {
                json obj;
                obj["jsonrpc"] = json("2.0");

                obj["method"] = json(msg.method);
                if (!msg.params.is_null()) {
                    obj["params"] = msg.params;
                }
                obj["id"] = msg.id;
                return json(obj).dump();
            }

            std::string serialize(const response_message &msg) {
                json obj;
                obj["jsonrpc"] = json("2.0");

                obj["result"] = msg.result;

                if (msg.error) {
                    json error = {
                            {"code", json(uint64_t(msg.error->code))},
                            {"message", json(msg.error->message)},
                            {"data", json(msg.error->data)}
                    };
                    obj["error"] = json(error);
                }

                obj["id"] = msg.id;
                return json(obj).dump();
            }

            std::string serialize(const notify_message &msg) {
                json obj;
                obj["jsonrpc"] = json("2.0");

                obj["method"] = json(msg.method);
                if (!msg.params.is_null()) {
                    obj["params"] = msg.params;
                }
                return json(obj).dump();
            }

            namespace request {
                std::string serialize(const json &id, const std::string &medhod,
                                      const json &param = json()) {
                    json obj;
                    obj["method"] = json(medhod);
                    if (!param.is_null()) {
                        obj["params"] = param;
                    }
                    obj["id"] = id;
                    return json(obj).dump();
                }

                std::string serialize(const json &id, const std::string &medhod,
                                      const std::string &param) {
                    return serialize(id, medhod, json(param));
                }

                std::string serialize(uint64_t id, const std::string &medhod,
                                      const std::string &param) {
                    return serialize(json(id), medhod, json(param));
                }

                std::string serialize(uint64_t id, const std::string &medhod,
                                      const json &param = json()) {
                    return serialize(json(id), medhod, json(param));
                }

                std::string serialize(const std::string &id, const std::string &medhod,
                                      const std::string &param) {
                    return serialize(json(id), medhod, json(param));
                }

                std::string serialize(const std::string &id, const std::string &medhod,
                                      const json &param = json()) {
                    return serialize(json(id), medhod, json(param));
                }
            }

            namespace notify {

                std::string serialize(const std::string &medhod,
                                      const json &param = json()) {
                    json obj;
                    obj["method"] = json(medhod);
                    if (!param.is_null()) {
                        obj["params"] = param;
                    }
                    return json(obj).dump();
                }

                std::string serialize(const std::string &medhod,
                                      const std::string &param) {
                    return serialize(medhod, json(param));
                }
            }

            namespace responce {

                std::string serialize(const json &id,
                                      const json &result = json(),
                                      bool error = false) {
                    json obj;
                    if (error) {
                        obj["error"] = result;
                    } else {
                        obj["result"] = result;
                    }
                    obj["id"] = id;
                    return json(obj).dump();
                }

                std::string serialize(const json &id, const std::string &result,
                                      bool error = false) {
                    return serialize(id, json(result), error);
                }

            }


        }
    }
}