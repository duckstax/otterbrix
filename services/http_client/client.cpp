#include <rocketjoe/services/http_client/client.hpp>
#include <rocketjoe/http/http.hpp>

#include <nlohmann/json.hpp>

#include <unordered_map>
#include <unordered_set>
#include <goblin-engineer/dynamic.hpp>


namespace rocketjoe { namespace services {


        client::client(goblin_engineer::dynamic_config &, goblin_engineer::abstract_environment *env) :abstract_service(env, "router") {


        }

        void client::startup(goblin_engineer::context_t *) {

        }

        void client::shutdown() {

        }

    }
}
