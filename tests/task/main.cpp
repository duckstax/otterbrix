#include <memory>

#include <actor-zeta/messaging/message.hpp>
#include <rocketjoe/api/json_rpc.hpp>
#include <rocketjoe/api/application.hpp>
#include <actor-zeta/behavior/context.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <RocketJoe/api/http.hpp>

class dummy final : public actor_zeta::actor::abstract_actor {
public:
    dummy(): actor_zeta::actor::abstract_actor(nullptr,""){

    }
    bool send(actor_zeta::messaging::message&&){

    }

    bool send(actor_zeta::messaging::message&&, actor_zeta::executor::execution_device *){

    }
};

int main () {
    dummy d;
    rocketjoe::api::task t;
    t.request.params["name"]="1qaz";
    t.transport_ = std::make_shared<rocketjoe::api::http>(1111111);
    auto msg =  actor_zeta::messaging::make_message(d.address(),"",std::move(t));

    auto l = [](actor_zeta::behavior::context &ctx) -> void {

        auto& t = ctx.message().body<rocketjoe::api::task>();
        auto app_name = t.request.params.at("name").get<std::string>();

        auto app_id = boost::uuids::to_string(boost::uuids::random_generator_mt19937()());
        auto app_key = boost::uuids::to_string(boost::uuids::random_generator_mt19937()());



        auto http = std::make_unique<rocketjoe::api::http>(t.transport_->id());

        rocketjoe::api::json_rpc::response_message response;
        response.id = t.request.id;
        response.result["application-id"] = app_id;
        response.result["api-key"] = app_key;

        http->body(rocketjoe::api::json_rpc::serialize(response));
        http->header("Content-Type", "application/json");

        rocketjoe::api::app_info app_info_(app_name,app_id,app_key);

    };

    {

        actor_zeta::behavior::context text_context(nullptr, std::move(msg));
        l(text_context);
    }

    return 0;
}*/