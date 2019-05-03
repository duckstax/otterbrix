#pragma once

#include <sstream>
#include <unordered_map>
#include <string>
#include <functional>

#include <actor-zeta/actor/actor_address.hpp>
#include <actor-zeta/messaging/message.hpp>
#include "forward.hpp"


namespace rocketjoe { namespace http {

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

        class response_context_type final {
        public:
            response_context_type() = default;
            response_context_type(const response_context_type&) = default; // todo hack
            response_context_type&operator=(const response_context_type&) = delete;
            response_context_type(response_context_type&&) = default;
            response_context_type&operator=(response_context_type&&) = default;
            ~response_context_type() = default;

            response_context_type(
                    response_type response_,
                    std::size_t i
            ):
                    response_(std::move(response_)),
                    id_(i){

            }

            response_context_type(
                    std::size_t i
            ):
                    id_(i){

            }


            auto response() ->  response_type& {
                return response_;
            }

            auto id() {
                return id_;
            }

        private:
            response_type response_;
            std::size_t id_;

        };


    class http_query_context final {
    public:
        http_query_context() = default;
        http_query_context(const http_query_context&) = default; // todo hack
        http_query_context&operator=(const http_query_context&) = delete;
        http_query_context(http_query_context&&) = default;
        http_query_context&operator=(http_query_context&&) = default;
        ~http_query_context() = default;

        http_query_context(
                request_type request_,
                size_t i,
                actor_zeta::actor::actor_address address
        ):
                request_(std::move(request_)),
                id_(i),
                address(std::move(address)){

        }

        auto request() ->  request_type& {
            return request_;
        }

        auto response() ->  response_type& {
            return response_;
        }

        auto id() {
            return id_;
        }

        /*
        auto response(response_type&&response_) {
            this->response_ = std::move(response_);
        }
        */
        auto write() {
            response_.prepare_payload();
            response_context_type context(std::move(response_),id_);
            address->send(
                    actor_zeta::messaging::make_message(
                       address,
                       "write",
                       std::move(context)

                    )
            );
        }

    private:
        request_type request_;
        response_type response_;
        actor_zeta::actor::actor_address address;
        std::size_t id_;

    };

    class http_method_container final {
    public:

        http_method_container() = default;
        http_method_container(http_method_container&&) = default;
        ~http_method_container() = default;

        using action = std::function<void(http_query_context&)>;
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

        auto invoke(http_query_context& r){
            auto url = r.request().target().to_string();
            auto it = storage_.find(url);
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
        ~router() = default;

        using storage = std::unordered_map<http_method,http_method_container,http_method_hasher>;
        using iterator = storage::iterator;

        template<class F>
        auto registration_handler(
                http_method method,
                string_view route_path,
                const options &options,
                F handler
        ) {

            auto it = storage_.find(method);
            if (it == storage_.end()) {
                auto result = storage_.emplace(method, http_method_container());
                result.first->second.registration_handler(route_path, options, std::forward<F>(handler));
            } else {
                it->second.registration_handler(route_path, options, std::forward<F>(handler));
            }
        }

        auto invoke(http_query_context&context){
            auto method = context.request().method();

            auto it = storage_.find(method);
            it->second.invoke(context);
        }

        auto update(router&r){
           for(auto&&i:r ){

           }
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

        auto get_router() -> router {
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

}}