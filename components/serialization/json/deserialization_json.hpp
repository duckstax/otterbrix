#pragma once

#include <cassert>
#include <iostream>

#include <boost/core/ignore_unused.hpp>
#include <boost/json.hpp>

#include <components/serialization/json/context_json.hpp>
#include <components/serialization/json/convert.hpp>
#include <components/serialization/traits.hpp>

namespace components::serialization::detail {

    void intermediate_deserialize_array(context::json_context& context, std::size_t size, const unsigned int version);
    void intermediate_deserialize_map(context::json_context& context, std::size_t size, const unsigned int version);

    template<typename Contaner>
    void intermediate_deserialize(context::json_context& context,
                               Contaner& data,
                               const unsigned int version,
                               traits::signed_number_tag) {
        boost::ignore_unused(version);
        assert(context.index_deserialization >= 0 && context.index_deserialization < context.number_of_elements);
        assert(context::detail::state_t::array == context.state_);
        if constexpr (std::is_same_v<int8_t, std::decay_t<Contaner>>) {
            data = static_cast<int8_t>(context.value_.as_array()[context.index_deserialization].as_int64());
        } else if constexpr (std::is_same_v<int16_t, std::decay_t<Contaner>>) {
            data = static_cast<int16_t>(context.value_.as_array()[context.index_deserialization].as_int64());
        } else if constexpr (std::is_same_v<int32_t, std::decay_t<Contaner>>) {
            data = static_cast<int32_t>(context.value_.as_array()[context.index_deserialization].as_int64());
        } else if constexpr (std::is_same_v<int64_t, std::decay_t<Contaner>>) {
            const auto& tmp = context.value_.as_array()[context.index_deserialization].as_int64();
            data = tmp;
        }
        ++context.index_deserialization;
    }

    template<typename Contaner>
    void intermediate_deserialize(context::json_context& context,
                               Contaner& data,
                               const unsigned int version,
                               traits::unsigned_number_tag) {
        boost::ignore_unused(version);
        assert(context.index_deserialization >= 0 && context.index_deserialization < context.number_of_elements);
        assert(context::detail::state_t::array == context.state_);
        if constexpr (std::is_same_v<uint8_t, std::decay_t<Contaner>>) {
            data = static_cast<uint8_t>(context.value_.as_array()[context.index_deserialization].as_uint64());
        } else if constexpr (std::is_same_v<uint16_t, std::decay_t<Contaner>>) {
            data = static_cast<uint16_t>(context.value_.as_array()[context.index_deserialization].as_uint64());
        } else if constexpr (std::is_same_v<uint32_t, std::decay_t<Contaner>>) {
            data = static_cast<uint32_t>(context.value_.as_array()[context.index_deserialization].as_uint64());
        } else if constexpr (std::is_same_v<uint64_t, std::decay_t<Contaner>>) {
            data = static_cast<uint64_t>(context.value_.as_array()[context.index_deserialization].as_uint64());
        }
        ++context.index_deserialization;
    }

    template<typename Contaner>
    void intermediate_deserialize(context::json_context& context,
                               Contaner& data,
                               const unsigned int version,
                               traits::char_tag) {
        boost::ignore_unused(version);
        assert(context.index_serialization > 0);
        assert(context::detail::state_t::array == context.state_);
        context.value_.as_array().emplace_back(boost::json::value(data));
        ++context.index_deserialization;
    }

    template<typename Contaner>
    void intermediate_deserialize(context::json_context& context,
                               Contaner& data,
                               const unsigned int version,
                               traits::boolean_tag) {
        boost::ignore_unused(version);
        assert(context.index_deserialization >= 0 && context.index_deserialization < context.number_of_elements);
        assert(context::detail::state_t::array == context.state_);
        data = context.value_.as_array()[context.index_deserialization].as_bool();
        ++context.index_deserialization;
    }

    template<class Contaner>
    void intermediate_deserialize(context::json_context& context,
                               Contaner& data,
                               const unsigned int version,
                               traits::string_tag) {
        boost::ignore_unused(version);
        assert(context.index_deserialization >= 0 && context.index_deserialization < context.number_of_elements);
        assert(context::detail::state_t::array == context.state_);
        const auto& str = context.value_.as_array()[context.index_deserialization].as_string();
        auto first = std::begin(str);
        auto last = std::end(str);
        std::copy(first,last,std::back_inserter(data));
        ++context.index_deserialization;
    }

    template<class Contaner>
    void intermediate_deserialize(context::json_context& context,
                               Contaner& data,
                               const unsigned int version,
                               traits::array_tag) {
        boost::ignore_unused(version);
        assert(context.index_deserialization >= 0 && context.index_deserialization < context.number_of_elements);
        assert(context::detail::state_t::array == context.state_);
        const auto& array = context.value_.as_array()[context.index_deserialization].as_array();
        auto first = std::begin(array);
        auto last = std::end(array);
        std::copy(first,last,std::back_inserter(data));
        ++context.index_deserialization;
    }

    template<class Contaner>
    void intermediate_deserialize(context::json_context& context,
                               Contaner& data,
                               const unsigned int version,
                               traits::object_tag) {
        boost::ignore_unused(version);
        assert(context.index_serialization > 0);
        assert(context::detail::state_t::array == context.state_);
        auto size = std::size(data);
        auto first = std::begin(data);
        auto last = std::end(data);
        boost::json::object object(size, context.value_.get_allocator().resource());
        for (auto it = first; it != last; ++it) {
            const auto& [key, value] = *it;
            object.emplace(detail::convert(key), value); /// TODO: value is simple type
        }
        context.value_.as_array().emplace_back(std::move(object));
        ++context.index_deserialization;
    }

} // namespace components::serialization::detail