#include "rocketjoe/http/router.hpp"

void init_http(){
    rocketjoe::http::wrapper_router router_;

    router_.http_get(
            "/ping",
            [](rocketjoe::http::request_context& request){

            }
    );
}