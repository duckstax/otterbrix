#pragma once

#include "transport_base.hpp"
#include <unordered_map>
#include <string>

namespace RocketJoe { namespace transport {

        enum class http_request_command {
            unknown = 0,
            delete_,
            get,
            head,
            post,
            put,
            connect,
            options,
            trace,
        };

        using url = std::string;

        struct http final : public transport_base {
            using header_storage = std::unordered_map<std::string, std::string>;
            using header_const_iterator = typename  header_storage::const_iterator;
            http(transport_id);
            virtual ~http();
            void header(std::string&&,std::string&&);
            std::pair<header_const_iterator,header_const_iterator> headers() const;
            void uri(const std::string&);
            void method(const std::string&);
            void body(const std::string&);
            std::string body() const;

            url uri_;
            std::string body_;
            std::string method_;
        private:
            header_storage headers_;

        };

    }
}