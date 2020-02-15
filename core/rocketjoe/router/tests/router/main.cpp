#include <iostream>
#include <memory>

#include <rocketjoe/services/router/router.hpp>

int main() {

    rocketjoe::services::detail::wrapper_router router_;

    router_.http_get(
            "/ping",
            [](rocketjoe::network::query_context  &/*r*/) {
                std::cout << " ping " << std::endl;
            }
    );

    
    auto r = router_.get_router();
    actor_zeta::actor::actor_address address;
    rocketjoe::network::request_type request;
    request.method(rocketjoe::network::http_method::get);
    request.target("/ping");
    rocketjoe::network::query_context query(std::move(request), 1, std::move(address));
    r.invoke(query);
    return 0;

}