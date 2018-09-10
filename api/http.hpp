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


        class http final : public transport_base {
        public:
            using header_storage = std::unordered_map<std::string, std::string>;
            using header_const_iterator = typename  header_storage::const_iterator;

            http(transport_id);
            ~http() override;
            ///header
            void header(std::string&&,std::string&&);
            std::pair<header_const_iterator,header_const_iterator> headers() const;
            ///header
            ///url
            void uri(const std::string&);
            const std::string& uri() const;
            ///url
            ///method
            void method(const std::string&);
            const std::string& method() const;
            ///method
            ///body
            void body(const std::string&);
            const std::string& body() const;
            ///body
            /// status code
            void status(unsigned code);
            unsigned status() const;
            ///status code

        private:
            unsigned status_code;
            std::string uri_;
            std::string method_;
            std::string body_;
            header_storage headers_;

        };

    }
}