#include <rocketjoe/http/http.hpp>

namespace rocketjoe { namespace api {

        http::http(transport_id id) : transport_base(transport_type::http, id) {

        }

        void http::header(std::string &&heder_name, std::string &&header_value) {
            headers_.emplace(std::move(heder_name), std::move(header_value));
        }

        void http::uri(const std::string &uri_) {
            this->uri_ = uri_;
        }

        void http::method(const std::string & method_) {
            this->method_= method_;
        }

        void http::body(const std::string & body_) {
            this->body_ = body_;
        }

        const std::string& http::body() const {
            return body_;
        }

        const std::string& http::uri() const {
            return this->uri_;
        }

        const std::string& http::method() const {
            return this->method_;
        }

        void http::status(unsigned code) {
            status_code = code;
        }

        unsigned http::status() const {
            return status_code;
        }

        void http::header(const char *key, const char *value) {
            headers_.emplace(key, value);
        }

        auto http::begin() -> http::header_iterator {
            return headers_.begin();
        }

        auto http::end() -> http::header_iterator {
            return headers_.end();
        }

        auto http::header(std::string &name) const -> const std::string & {
            return headers_.at(name);
        }

    }
}