#include "json_rpc.hpp"

namespace rocketjoe { namespace api { namespace json_rpc {

            bool contains(const json &msg, const std::string &key) {
                    return msg.find(key) != msg.end();
            }

            bool is_notify(const json &msg) {
                    return msg.is_object() && !contains(msg, "id");
            }

            bool is_request(const json &msg) {
                    return msg.is_object() && contains(msg,"method") && msg.at("method").is_string();
            }

            bool is_response(const json &msg) {
                    return msg.is_object() && !contains(msg, "method") && contains(msg, "id");
            }



bool parse(const std::string &raw, request_message &request) {

        auto message = json::parse(raw);

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

        if (contains(message, "api-key")) {
                request.api_key = message["api-key"].get<std::string>();
        }

        if (contains(message, "application-id")) {
                request.application_id = message["application-id"].get<std::string>();
        }

        return true;

}

bool parse(const nlohmann::json &message, notify_message &notify) {

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

bool parse(const nlohmann::json &message, response_message &response) {

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

}}}