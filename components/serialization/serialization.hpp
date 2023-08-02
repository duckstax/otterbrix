#pragma once

#include <components/serialization/stream/base.hpp>
#include <components/serialization/traits.hpp>
#include <core/pmr.hpp>

#include <boost/core/ignore_unused.hpp>
#include <experimental/memory_resource>
#include <map>
#include <vector>

namespace components::serialization {

    struct serialize_to_array_t {};
    struct serialize_to_map_t {};

    constexpr serialize_to_array_t serialize_to_array{};
    constexpr serialize_to_map_t serialize_to_map{};

    template<class Storage, template<class T> class stream>
    void serialize(stream<Storage>& ar, serialize_to_array_t rules, std::size_t size) {
        intermediate_serialize_array(ar, size, 0);
    }

    template<class Storage, template<class T> class stream>
    void serialize(stream<Storage>& ar, serialize_to_map_t rules, std::size_t size) {
        intermediate_serialize_map(ar, size, 0);
    }

    template<class Storage, template<class T> class stream, class Contaner>
    void serialize(stream<Storage>& ar, Contaner& data) {
        intermediate_serialize(
            ar,
            data,
            0,
            typename serialization_trait<stream<Storage>, Contaner>::category{});
    }

} // namespace components::serialization