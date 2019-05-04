#include <iostream>
#include <memory>

#include <rocketjoe/http/router.hpp>

int main() {

    rocketjoe::http::wrapper_router router_;

    router_.http_get(
            "/ping",
            [](rocketjoe::http::query_context  &r) {
                std::cout << " ping " << std::endl;
            }
    );

    
    auto r = std::move(router_.get_router());
    actor_zeta::actor::actor_address address;
    rocketjoe::http::request_type request;
    request.method(rocketjoe::http::http_method::get);
    request.target("/ping");
    rocketjoe::http::query_context query(std::move(request), 1, std::move(address));
    r.invoke(query);
    return 0;

}