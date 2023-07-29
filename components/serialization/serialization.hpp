#pragma once

#include <components/serialization/stream/base.hpp>
#include <components/serialization/traits.hpp>
#include <core/pmr.hpp>

#include <boost/core/ignore_unused.hpp>
#include <map>
#include <vector>
#include <experimental/memory_resource>

std::experimental::memory_resource;

namespace components::serialization {

    template<class Storage, template<class T> class stream,class Rules>
    void serialize(stream<Storage>& ar,Rules rules, std::size_t size) {
        intermediate_serialize_array(ar, size, version);
    }

    template<class Storage, template<class T> class stream,class Rules>
    void serialize(stream<Storage>& ar,Rules rules, std::size_t size) {
        serialize_map(ar, size, 0);
    }

    ///####### anonyms serialize

    template<class Storage,template<class T> class stream,class Contaner>
    void serialize(stream<Storage>& ar, const Contaner& data, const unsigned int version) {
        intermediate_serialize(
            ar,
            std::size(data),
            std::begin(data),
            std::end(data),
            version,
            typename serialization_trait<stream<Storage>,Contaner>::category{});
    }

    template<class Storage, template<class T> class stream,class Contaner>
    void serialize(stream<Storage>& ar, const Contaner& data) {
        serialize(ar,data,0);
    }

    ///####### anonyms serialize

    ///####### name serialize

    ///####### name serialize
} // namespace components::serialization