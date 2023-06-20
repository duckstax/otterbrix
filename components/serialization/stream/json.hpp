#pragma once
#include "base.hpp"

#include <boost/json.hpp>

#include <boost/core/ignore_unused.hpp>
#include <map>
#include <variant>
#include <vector>

namespace components::serialization::stream {

    template<>
    class stream<boost::json::value> {
    public:
        stream()
            : parent_(boost::json::value()) {}
        ~stream() = default;

        [[nodiscard]] inline std::string data() const {
            return boost::json::serialize(parent_);
        }

        inline boost::json::array& array() {
            if (current_.index() == 0) {
                boost::json::array& reference = parent_.emplace_array();
                current_ = reference;
                return reference;
            } else {
                boost::json::array& reference = std::get<std::reference_wrapper<boost::json::array>>(current_);
                return reference;
            }
        }

        inline boost::json::object& object() {
            if (current_.index() == 0) {
                auto& ref = parent_.emplace_object();
                current_ = ref;
                return ref;
            } else {
                auto& ref = std::get<std::reference_wrapper<boost::json::object>>(current_);
                return ref;
            }
        }

        enum class state {
            init,
            array,
            object
        } state_ = state::init;

        boost::json::value parent_;
        std::variant<std::monostate, std::reference_wrapper<boost::json::object>, std::reference_wrapper<boost::json::array>> current_;
        std::size_t size_{0};
    };

    using stream_json = stream<boost::json::value>;

    void intermediate_serialize_array(stream_json& ar, std::size_t size, const unsigned int version);
    void intermediate_serialize_map(stream_json& ar, std::size_t size, const unsigned int version);
    void intermediate_serialize(stream_json& ar, uint64_t data, const unsigned int version);
    void intermediate_serialize(stream_json& ar, const std::string& data, const unsigned int version);
    template<class T>
    void intermediate_serialize(stream_json& ar, const std::vector<T> data, const unsigned int version){

    }

} // namespace components::serialization::stream