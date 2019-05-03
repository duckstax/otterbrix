#include <rocketjoe/data_provider/http/router.hpp>
#include <iostream>

int main() {

    rocketjoe::data_provider::http::wrapper_router router_;

    router_.http_get(
            "/ping",
            [](rocketjoe::data_provider::http::request_context &r) {
                std::cout << " ping " << std::endl;
            }
    );

    
    auto r = std::move(router_.get_router());
    actor_zeta::actor::actor_address address;
    rocketjoe::data_provider::http::request_type request;
    request.method(rocketjoe::data_provider::http::http_method::get);
    request.target("/ping");
    r.invoke(std::move(request), 1, std::move(address));
    return 0;

}