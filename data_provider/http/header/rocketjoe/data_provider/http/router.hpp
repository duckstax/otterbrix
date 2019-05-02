#pragma once

#include <sstream>
#include <unordered_map>
#include <string>
#include <functional>

#include <actor-zeta/actor/actor_address.hpp>

#include <boost/beast/http/verb.hpp>
#include <boost/beast/http.hpp>

namespace rocketjoe { namespace data_provider { namespace http {

    using boost::string_view;
    using http_method = boost::beast::http::verb ;
    using request_type = boost::beast::http::request<boost::beast::http::string_body>;
    using response = boost::beast::http::response<boost::beast::http::string_body>;

    class options final {
    public:
        constexpr  options()= default;
        ~options() = default;
    };

    struct http_method_hasher final {
        std::size_t operator()(const http_method &k) const {
            using std::size_t;
            using std::hash;

            return (hash<uint64_t >()(static_cast<uint64_t>(k)));
        }
    };

    struct url_hasher final {
        std::size_t operator()(const string_view &k) const {
            return boost::hash_value(k);
        }
    };

    class request_context final {
    public:
        request_context() = delete;
        request_context(const request_context&) = delete;
        request_context&operator=(const request_context&) = delete;
        request_context(request_context&&) = default;
        request_context&operator=(request_context&&) = default;
        ~request_context() = default;

        request_context(
                request_type request_,
                size_t i,
                actor_zeta::actor::actor_address address
        ):
                request_(std::move(request_)),
                id(i),
                address(std::move(address)){

        }

        auto request() ->  request_type& {
            return request_;
        }

    private:
        request_type request_;
        actor_zeta::actor::actor_address address;
        std::size_t id;

    };

    class http_method_container final {
    public:

        http_method_container() = default;
        http_method_container(http_method_container&&) = default;

        using action = std::function<void(request_context&)>;
        using storage = std::unordered_map<std::string,action>;
        using iterator = storage::iterator;

        template <class F>
        auto registration_handler(
                string_view route_path,
                const options &options,
                F handler
        ){
            storage_.emplace(route_path.to_string(),std::forward<F>(handler));
        }

        auto invoke(request_context r){
            auto it = storage_.find(r.request().target().to_string());
            auto request = std::move(r);
            it->second(request);
        }

        auto update(http_method_container&r){
            storage_.insert(
                    std::make_move_iterator(r.begin()),
                    std::make_move_iterator(r.end())
            );
        }

        auto begin() -> iterator {
            return storage_.begin();
        }

        auto end() -> iterator {
            return storage_.end();
        }

    private:
        storage storage_;
    };

    class router final {
    public:
        router() = default;
        router(router&&) = default;
        router&operator =(router&&) = default;

        using storage = std::unordered_map<http_method,http_method_container,http_method_hasher>;
        using iterator = storage::iterator;

        template <class F>
        auto registration_handler(
                http_method method,
                string_view route_path,
                const options &options,
                F handler
        ){

            auto it = storage_.find(method);
            if(it == storage_.end()){
                storage_.emplace(method,http_method_container());
            }

            it->second.registration_handler(route_path,options,std::forward<F>(handler));

        }

        auto invoke(request_type r, std::size_t id,actor_zeta::actor::actor_address address){
            auto method = r.method();

            auto it = storage_.find(method);
            request_context context(std::move(r),id,std::move(address));
            it->second.invoke(std::move(context));
        }

        auto update(router&r){
            storage_.insert(
                    std::make_move_iterator(r.begin()),
                    std::make_move_iterator(r.end())
            );
        }

        auto begin() -> iterator {
            return storage_.begin();
        }

        auto end() -> iterator {
            return storage_.end();
        }

    private:
        storage storage_;
    };

    class wrapper_router final {
    public:
        wrapper_router() = default;

        wrapper_router(wrapper_router &&) = default;

        ~wrapper_router() = default;

        router get_router(){
///            assert()
            return std::move(router_);
        }

        template<typename F>
        auto http_delete(
                string_view route_path,
                F handler
        ) {
            add_handler(
                    http_method::delete_,
                    route_path,
                    std::move(handler)
            );
        }

        template<typename F>
        auto http_delete(
                string_view route_path,
                const options &options,
                F handler
        ) {
            add_handler(
                    http_method::delete_,
                    route_path,
                    options,
                    std::move(handler)
            );
        }

        template<typename F>
        auto http_get(
                string_view route_path,
                F handler
        ) {
            add_handler(
                    http_method::get,
                    route_path,
                    std::forward<F>(handler)
            );
        }

        template<typename F>
        auto http_get(
                string_view route_path,
                const options &options,
                F handler
        ) {
            add_handler(
                    http_method::get,
                    route_path,
                    options,
                    std::move(handler)
            );
        }

        template<typename F>
        auto http_head(
                string_view route_path,
                F handler
        ) {
            add_handler(
                    http_method::head,
                    route_path,
                    std::move(handler)
            );
        }

        template<typename F>
        auto http_head(
                string_view route_path,
                const options &options,
                F handler
        ) {
            add_handler(
                    http_method::head,
                    route_path,
                    options,
                    std::move(handler)
            );
        }

        template<typename F>
        auto http_post(
                string_view route_path,
                F handler
        ) {
            add_handler(
                    http_method::post,
                    route_path,
                    std::forward<F>(handler)
            );
        }

        template<typename F>
        auto http_post(
                string_view route_path,
                const options &options,
                F handler
        ) {
            add_handler(
                    http_method::post,
                    route_path,
                    options,
                    std::move(handler)
            );
        }

        template<typename F>
        auto http_put(
                string_view route_path,
                F handler
        ) {
            add_handler(
                    http_method::put,
                    route_path,
                    std::move(handler)
            );
        }

        template<typename F>
        auto http_put(
                string_view route_path,
                const options &options,
                F handler
        ) {
            add_handler(
                    http_method::put,
                    route_path,
                    options,
                    std::forward<F>(handler)
            );
        }

    private:

        template<typename F>
        auto add_handler(
                http_method method,
                string_view route_path,
                F handler
        ) {
            add_handler(
                    method,
                    route_path,
                    options{},
                    std::forward<F>(handler)
            );
        }

        template<typename F>
        auto add_handler(
                http_method method,
                string_view route_path,
                const options &options,
                F handler
        ) {
            router_.registration_handler(method, route_path, options, std::forward<F>(handler));
        }

        router router_;
    };

}}}