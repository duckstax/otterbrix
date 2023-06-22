#pragma once
#include "base.hpp"

#include <boost/json.hpp>

#include <boost/core/ignore_unused.hpp>
#include <iostream>
#include <map>
#include <variant>
#include <vector>

namespace components::serialization::stream {
    namespace detail {

        enum class state_t {
            init,
            array,
            object
        };

    } // namespace detail

    template<>
    class stream<boost::json::value> {
    public:
        stream()
            : value_(boost::json::value()) {}
        ~stream() = default;

        [[nodiscard]] inline std::string data() const {
            return boost::json::serialize(value_);
        }

        detail::state_t state_{detail::state_t::init};
        std::size_t size_{0};
        boost::json::value value_;
    };

    using stream_json = stream<boost::json::value>;

    namespace detail {

        template<class T>
        std::string convert(T& t) {
            return std::to_string(t);
        }

        std::string convert(const std::string& t);
        std::string convert(std::string_view t);

    } // namespace detail

    void intermediate_serialize_array(stream_json& ar, std::size_t size, const unsigned int version);
    void intermediate_serialize_map(stream_json& ar, std::size_t size, const unsigned int version);
    void intermediate_serialize(stream_json& ar, uint64_t data, const unsigned int version);
    void intermediate_serialize(stream_json& ar, int64_t data, const unsigned int version);
    void intermediate_serialize(stream_json& ar, const std::string& data, const unsigned int version);
    void intermediate_serialize(stream_json& ar, std::string_view data, const unsigned int version);

    template<class T>
    void intermediate_serialize(stream_json& ar, const std::vector<T>& data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        boost::json::array array(ar.value_.get_allocator().resource());
        array.reserve(data.size());
        for (const auto& item : data) {
            array.emplace_back(item);
        }
        ar.value_.as_array().emplace_back(array);
        ar.size_--;
    }

    template<class Key, class Value>
    void intermediate_serialize(stream_json& ar, const std::map<Key, Value>& data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        boost::json::object object(ar.value_.get_allocator().resource());
        for (const auto& [key, value] : data) {
            object.emplace(detail::convert(key), value);
        }
        ar.value_.as_array().emplace_back(object);
        ar.size_--;
    }

} // namespace components::serialization::stream