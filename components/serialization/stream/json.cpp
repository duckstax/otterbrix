#include "json.hpp"
namespace components::serialization::stream {
    void intermediate_serialize_array(stream_json& ar, std::size_t size, const unsigned int version) {
        assert(stream_json::state::init == ar.state_);
        ar.size_ = size;
        ar.parent_ = boost::json::array();
        ar.state_ = stream_json::state::array;
        ar.array();
    }

    void intermediate_serialize_map(stream_json& ar, std::size_t size, const unsigned int version) {
        assert(stream_json::state::init == ar.state_);
        ar.size_ = size;
        ar.parent_ = boost::json::object();
        ar.state_ = stream_json::state::object;
        ar.object();
    }

    void intermediate_serialize(stream_json& ar, uint64_t data, const unsigned int version) {
        assert(ar.size_ > 0);
        assert(stream_json::state::array == ar.state_);
        ar.array().emplace_back(data);
        ar.size_--;
    }

    /// string
    void intermediate_serialize(stream_json& ar, const std::string& data, const unsigned int version) {
        assert(ar.size_ > 0);
        assert(stream_json::state::array == ar.state_);
        ar.array().emplace_back(data);
        ar.size_--;
    }
} // namespace components::serialization::stream