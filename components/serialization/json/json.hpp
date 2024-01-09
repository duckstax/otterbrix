#pragma once

#include <cassert>
#include <iostream>
#include <map>
#include <variant>
#include <vector>

#include <components/serialization/context.hpp>
#include <components/serialization/traits.hpp>

#include <boost/core/ignore_unused.hpp>
#include <boost/json.hpp>

namespace components::serialization {

    namespace context {
        namespace detail {

            enum class state_t { init, array, object };

        } // namespace detail

        template<>
        class context_t<boost::json::value> {
        public:
            context_t() = default;
            ~context_t() = default;

            [[nodiscard]] inline std::string data() const { return boost::json::serialize(value_); }

            detail::state_t state_{detail::state_t::init};
            std::size_t size_{0};
            boost::json::value value_{boost::json::value()};
        };

        using json_context = context_t<boost::json::value>;
    } // namespace context

    namespace detail {
        template<class T>
        std::string convert(T& t) {
            return std::to_string(t);
        }

        std::string convert(const std::string& t);
        std::string convert(std::string_view t);
    } // namespace detail

    namespace detail {
        void intermediate_serialize_array(context::json_context& context, std::size_t size, const unsigned int version) ;
        void intermediate_serialize_map(context::json_context& context, std::size_t size, const unsigned int version) ;

        template<typename Contaner>
        void intermediate_serialize(context::json_context& context, Contaner& data,  const unsigned int version,traits::pod_tag) {
             boost::ignore_unused(version);
            assert(context.size_ > 0);
            assert(context::detail::state_t::array == ar.state_);
            context.value_.as_array().emplace_back(std::move(boost::json::value(data)));
            context.size_--;
        }

    template<class Contaner>
    void intermediate_serialize(context::json_context& ar,Contaner& data,const unsigned int version,traits::string_tag) {
            boost::ignore_unused(version);
            assert(ar.size_ > 0);
            assert(context::detail::state_t::array == ar.state_);
            auto size = std::size(data);
            auto first = std::begin(data);
            auto last = std::end(data);
            boost::json::string string(ar.value_.get_allocator().resource());
            string.assign(first, last);
            ar.value_.as_array().emplace_back(std::move(string));
            ar.size_--;
        }

    template<class Contaner>
    void intermediate_serialize(context::json_context& context,Contaner& data,const unsigned int version,traits::array_tag) {
            boost::ignore_unused(version);
            assert(context.size_ > 0);
            assert(context::detail::state_t::array == context.state_);
            boost::json::array array(context.value_.get_allocator().resource());
            auto size = std::size(data);
            auto first = std::begin(data);
            auto last = std::end(data);
            array.reserve(size);
            for (; first != last; ++first) {
                array.emplace_back(*first);
            }
            context.value_.as_array().emplace_back(std::move(array));
            context.size_--;
        }

        template<class Contaner>
        void intermediate_serialize(context::json_context& ar,Contaner& data,const unsigned int version,traits::object_tag) {
            boost::ignore_unused(version);
            assert(ar.size_ > 0);
            assert(context::detail::state_t::array == ar.state_);
            auto size = std::size(data);
            auto first = std::begin(data);
            auto last = std::end(data);
            boost::json::object object(size, ar.value_.get_allocator().resource());
            for (auto it = first; it != last; ++it) {
                const auto& [key, value] = *it;
                object.emplace(detail::convert(key), value); /// TODO: value is simple type
            }
            ar.value_.as_array().emplace_back(std::move(object));
            ar.size_--;
        }

        } // namespace detail

} // namespace components::serialization