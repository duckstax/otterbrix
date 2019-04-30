#pragma once

//#include <string>
#include <sstream>
//#include <iostream>
#include <unordered_map>
#include <string>

#include <boost/beast/http/verb.hpp>

namespace rocketjoe { namespace data_provider { namespace http {

    using boost::string_view;
    using http_method = boost::beast::http::verb ;

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

    struct http__context final {
        http__context() = delete;
        http__context(const http__context&) = delete;
        http__context&operator=(const http__context&) = delete;
        http__context(http__context&&) = delete;
        http__context&operator=(http__context&&) = delete;

    };

    class http_method_container final {
    public:

        http_method_container() = default;
        http_method_container(http_method_container&&) = default;

        using action = std::function<void(http__context)>;
        using storage = std::unordered_map<std::string,action>;

        template <class F>
        auto registration_handler(
                string_view route_path,
                const options &options,
                F handler
        ){
            storage_.emplace(route_path.to_string(),std::forward<F>(handler));
        }
    private:
        storage storage_;
    };

    class router final {
    public:
        router() = default;
        router(router&&) = default;

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

        auto invoke(){

        }
/*
        auto update(router&r){
            storage_.insert(r.begin(),r.end());
        }
*/
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

//        router get_router(){
///            assert()
//            return router_;
//        }

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