#include "rocketjoe/data_provider/http/router.hpp"

void init_http(){
    rocketjoe::data_provider::http::wrapper_router router_;

    router_.http_get(
            "/ping",
            [](rocketjoe::data_provider::http::request_context request){

            }
    );
}