#include "json.hpp"
#include <iostream>
namespace components::serialization::stream {

    namespace detail {
        std::string convert(const std::string& t) {
            return std::string(t);
        }

        std::string convert(std::string_view t) {
            return std::string(t.data(), t.size());
        }
    } // namespace detail

    void intermediate_serialize_array(output_stream_json& ar, std::size_t size, const unsigned int version) {
        boost::ignore_unused(version);
        assert(detail::state_t::init == ar.state_);
        ar.size_ = size;
        ar.value_ = boost::json::array();
        ar.state_ = detail::state_t::array;
    }

    void intermediate_serialize_map(output_stream_json& ar, std::size_t size, const unsigned int version) {
        boost::ignore_unused(version);
        assert(detail::state_t::init == ar.state_);
        ar.size_ = size;
        ar.value_ = boost::json::object();
        ar.state_ = detail::state_t::object;
    }

    void intermediate_serialize(output_stream_json& ar, std::string_view key, bool data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        ar.value_.as_object().emplace(key, data);
        ar.size_--;
    }

    void intermediate_serialize(output_stream_json& ar, std::string_view key, uint8_t data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        ar.value_.as_object().emplace(key, data);
        ar.size_--;
    }

    void intermediate_serialize(output_stream_json& ar, std::string_view key, int8_t data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        ar.value_.as_object().emplace(key, data);
        ar.size_--;
    }

    void intermediate_serialize(output_stream_json& ar, std::string_view key, uint16_t data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        ar.value_.as_object().emplace(key, data);
        ar.size_--;
    }

    void intermediate_serialize(output_stream_json& ar, std::string_view key, int16_t data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        ar.value_.as_object().emplace(key, data);
        ar.size_--;
    }

    void intermediate_serialize(output_stream_json& ar, std::string_view key, uint32_t data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        ar.value_.as_object().emplace(key, data);
        ar.size_--;
    }

    void intermediate_serialize(output_stream_json& ar, std::string_view key, int32_t data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::object == ar.state_);
        ar.value_.as_object().emplace(key, data);
        ar.size_--;
    }

} // namespace components::serialization::stream