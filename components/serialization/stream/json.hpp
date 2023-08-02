#pragma once
#include "base.hpp"

#include <boost/json.hpp>

#include <boost/core/ignore_unused.hpp>
#include <components/serialization/traits.hpp>
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
    class output_stream<boost::json::value> {
    public:
        output_stream()
            : value_(boost::json::value()) {}
        ~output_stream() = default;

        [[nodiscard]] inline std::string data() const {
            return boost::json::serialize(value_);
        }

        detail::state_t state_{detail::state_t::init};
        std::size_t size_{0};
        boost::json::value value_;
    };

    using output_stream_json = output_stream<boost::json::value>;

    namespace detail {

        template<class T>
        std::string convert(T& t) {
            return std::to_string(t);
        }

        std::string convert(const std::string& t);
        std::string convert(std::string_view t);

    } // namespace detail

    void intermediate_serialize_array(output_stream_json& ar, std::size_t size, const unsigned int version);
    void intermediate_serialize_map(output_stream_json& ar, std::size_t size, const unsigned int version);

    template<class T>
    void intermediate_serialize(output_stream_json& ar, T& data, const unsigned int version, pod_tag) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        ar.value_.as_array().emplace_back(data);
        ar.size_--;
    }

    template<class Contaner>
    void intermediate_serialize(output_stream_json& ar, Contaner& data, const unsigned int version, string_tag) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        auto size = std::size(data);
        auto first = std::begin(data);
        auto last = std::end(data);
        boost::json::string string(ar.value_.get_allocator().resource());
        string.assign(first, last);
        ar.value_.as_array().emplace_back(std::move(string));
        ar.size_--;
    }

    template<class Contaner>
    void intermediate_serialize(output_stream_json& ar, Contaner& data, const unsigned int version, array_tag) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        boost::json::array array(ar.value_.get_allocator().resource());
        auto size = std::size(data);
        auto first = std::begin(data);
        auto last = std::end(data);
        array.reserve(size);
        for (; first != last; ++first) {
            array.emplace_back(*first);
        }
        ar.value_.as_array().emplace_back(std::move(array));
        ar.size_--;
    }

    template<class Contaner>
    void intermediate_serialize(output_stream_json& ar, Contaner& data, const unsigned int version, object_tag) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
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
    /*
    void intermediate_serialize(output_stream_json& ar, std::string_view key, bool data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, uint8_t data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, int8_t data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, uint16_t data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, int16_t data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, uint32_t data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, int32_t data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, uint64_t data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, int64_t data, const unsigned int version);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, const std::string& data, const unsigned int version, string_tag);
    void intermediate_serialize(output_stream_json& ar, std::string_view key, std::string_view data, const unsigned int version, string_tag);

    template<class T>
    void intermediate_serialize(output_stream_json& ar, std::string_view key, const std::vector<T>& data, const unsigned int version, array_tag) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        boost::json::array array(ar.value_.get_allocator().resource());
        array.reserve(data.size());
        for (const auto& item : data) {
            array.emplace_back(item);
        }
        ar.value_.as_object().emplace(key, array);
        ar.size_--;
    }

    template<class Key, class Value>
    void intermediate_serialize(output_stream_json& ar, std::string_view key1, const std::map<Key, Value>& data, const unsigned int version, object_tag) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        boost::json::object object(ar.value_.get_allocator().resource());
        for (const auto& [key, value] : data) {
            object.emplace(detail::convert(key), value);
        }
        ar.value_.as_object().emplace(key1, object);
        ar.size_--;
    }*/

} // namespace components::serialization::stream