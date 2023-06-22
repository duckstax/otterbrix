#include "json.hpp"
#include <iostream>
namespace components::serialization::stream {

    namespace detail {
        std::string convert(const std::string& t){
            return std::string(t);
        }

        std::string convert(std::string_view t){
            return std::string(t.data(),t.size());
        }
    }

    void intermediate_serialize_array(stream_json& ar, std::size_t size, const unsigned int version) {
        boost::ignore_unused(version);
        assert(detail::state_t::init == ar.state_ );
        ar.size_ = size;
        ar.value_ = boost::json::array();
        ar.state_ = detail::state_t::array;
    }

    void intermediate_serialize_map(stream_json& ar, std::size_t size, const unsigned int version) {
        boost::ignore_unused(version);
        assert(detail::state_t::init == ar.state_);
        ar.size_ = size;
        ar.value_ = boost::json::object();
        ar.state_ = detail::state_t::object;
    }

    void intermediate_serialize(stream_json& ar, uint64_t data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        ar.value_.as_array().emplace_back(data);
        ar.size_--;
    }

    void intermediate_serialize(stream_json& ar, int64_t data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        ar.value_.as_array().emplace_back(data);
        ar.size_--;
    }

    void intermediate_serialize(stream_json& ar, const std::string& data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        ar.value_.as_array().emplace_back(data);
        ar.size_--;
    }

    void intermediate_serialize(stream_json& ar, const std::string_view data, const unsigned int version) {
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(detail::state_t::array == ar.state_);
        ar.value_.as_array().emplace_back(data);
        ar.size_--;
    }

} // namespace components::serialization::stream