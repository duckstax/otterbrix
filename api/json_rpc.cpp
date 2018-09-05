#include "json_rpc.hpp"

#include <nlohmann/json.hpp>


namespace network{ namespace protocol { namespace json_rpc {

/*
            auto json_rpc::parser(const std::string&data, message&message) -> bool {

                auto j = nlohmann::json::parse(data);

                if(j.is_array()){
                    return false;
                }

                auto& id = j.at("id");
                id.is_null();


                return true;

                return false;
            }

            auto json_rpc::parser(const std::string &data, batch &b) -> bool {

                auto j = nlohmann::json::parse(data);

                if(j.is_object()){
                    return false;
                }

                return false;
            }
*/
        }}}