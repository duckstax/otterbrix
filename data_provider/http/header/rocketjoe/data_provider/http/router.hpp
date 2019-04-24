#pragma once

//#include <string>
#include <sstream>
//#include <iostream>
#include <unordered_map>

#include <boost/beast/http/verb.hpp>

namespace rocketjoe { namespace data_provider { namespace http {

    using boost::string_view;
    using http_method = boost::beast::http::verb ;

    class options final {
    public:
        constexpr  options()= default;
        ~options() = default;
    };

    struct KeyHasher final {
        std::size_t operator()(const http_method &k) const {
            using std::size_t;
            using std::hash;
            using std::string;

            return (hash<uint64_t >()(static_cast<uint64_t>(k)));
        }

        std::size_t operator()(const string_view &k) const {
            return boost::hash_value(k);
        }
    };


    class router final {
    public:
        using storage = std::unordered_map<http_method,std::unordered_map<string_view,std::function<void()>,KeyHasher>,KeyHasher> ;
        using iterator = storage::iterator;
        template <class F>
        auto registration_handler(
                http_method method,
                string_view route_path,
                const options &options,
                F handler
        ){

        }

        auto update(router&r){
            storage_.insert(r.begin(),r.end());
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
            return router_;
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
                    std::move(handler)
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
                    std::move(handler)
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
                    std::move(handler)
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
                    std::move(handler)
            );
        }

        template<typename F>
        auto add_handler(
                http_method method,
                string_view route_path,
                const options &options,
                F handler
        ) {
            router_.registration_handler(method, route_path, options, std::move(handler));
        }

        router router_;
    };

}}}