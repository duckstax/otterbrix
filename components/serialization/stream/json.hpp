#pragma once
#include "base.hpp"

#include <boost/json.hpp>

#include <map>
#include <vector>

namespace components::serialization::stream {

    template<>
    class stream<boost::json::value> {
    public:
        stream() = default;
        ~stream() = default;

        inline std::string data() {
            return boost::json::value_to<std::string>(value_);
        }

        inline boost::json::value& value() {
            return value_;
        }

        inline boost::json::array& array() {
            return value_.emplace_array();
        }

        inline boost::json::object& object() {
            return value_.emplace_object();
        }

        boost::json::value value_;
    };

    using stream_json = stream<boost::json::value>;

    void intermediate_serialize(stream_json& ar, uint64_t& t, const unsigned int version) {
        ar.value() = t;
    }

    void intermediate_serialize(stream_json& ar, std::string& t, const unsigned int version) {
        ar.value() = t;
    }

    template<class T>
    void intermediate_serialize(stream_json& ar, std::vector<T>& t, const unsigned int version) {
        auto& array = ar.array();
        for (auto& item : t) {
            array.emplace_back(item);
        }
    }

    template<class K, class V>
    void intermediate_serialize(stream_json& ar, std::map<K, V>& t, const unsigned int version) {
        auto& object = ar.object();
        for (auto& [key, value] : t) {
           /// object.emplace(key, value);
        }
    }

} // namespace components::serialization::stream