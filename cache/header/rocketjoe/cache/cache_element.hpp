#pragma once

#include <string>
#include <actor-zeta/detail/intrusive_ptr.hpp>

namespace rocketjoe { namespace api {

        enum class cache_element_type : char {
            none = 0x00,
            string
        };

        struct cache_element_base : public actor_zeta::ref_counted  {
            cache_element_base(cache_element_type type_) : type_(type_) {}

            cache_element_type type_;

            virtual ~cache_element_base() = default;
        };

        using cache_element = actor_zeta::intrusive_ptr<cache_element_base>;


        class cache_element_string final : cache_element_base {
        public:
            cache_element_string() : cache_element_base(cache_element_type::string) {
            }

            cache_element_string(const std::string &body) :
                    cache_element_base(cache_element_type::string),
                    body(body) {
            }

            cache_element_string(char* body) :
                    cache_element_base(cache_element_type::string),
                    body(body) {
            }

            ~cache_element_string() = default;

            std::string body;
        };


}}