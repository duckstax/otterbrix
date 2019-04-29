#include "rocketjoe/data_provider/http/router.hpp"

void init_http(){
    rocketjoe::data_provider::http::wrapper_router router;
    constexpr const char* d = "/";
    router.http_get(
            d,
            [](rocketjoe::data_provider::http::http__context){

            }
    );
}