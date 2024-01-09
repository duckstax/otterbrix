#include "json.hpp"

namespace components::serialization {
    namespace detail {
        std::string convert(const std::string& t) { return std::string(t); }

        std::string convert(std::string_view t) { return std::string(t.data(), t.size()); }
    } // namespace detail
namespace detail {

    void intermediate_serialize_array(context::json_context& ar, std::size_t size, const unsigned int version) {
        boost::ignore_unused(version);
        assert(context::detail::state_t::init == ar.state_);
        ar.size_ = size;
        ar.value_ = boost::json::array();
        ar.state_ = context::detail::state_t::array;
    }


    void intermediate_serialize_map(context::json_context& ar, std::size_t size, const unsigned int version) {
        boost::ignore_unused(version);
        assert(context::detail::state_t::init == ar.state_);
        ar.size_ = size;
        ar.value_ = boost::json::object();
        ar.state_ = context::detail::state_t::object;
    }
/*
    void intermediate_serialize(context::json_context& ar, uint64_t& data, const unsigned int version, traits::pod_tag) {
        std::cerr << "void intermediate_serialize(output_stream_json& ar, T& data, const unsigned int version, "
                     "traits::pod_tag) {"
                  << std::endl;
        boost::ignore_unused(version);
        assert(ar.size_ > 0);
        assert(context::detail::state_t::array == ar.state_);
        std::cerr << "assert and assert;" << std::endl;
        std::cerr << typeid(data).name() << std::endl;
        std::cerr << data << std::endl;
        ar.value_.as_array().emplace_back(std::move(boost::json::value(data)));
        std::cerr << "ar.value_.as_array().emplace_back(data);" << std::endl;
        ar.size_--;
    }*/
}
} // namespace components::serialization