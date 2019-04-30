#include "rocketjoe/data_provider/http/router.hpp"

void init_http(){
    rocketjoe::data_provider::http::wrapper_router router;

    router.http_get(
            "/",
            [](rocketjoe::data_provider::http::http__context){

            }
    );
}