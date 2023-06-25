#pragma once

#include <components/serialization/stream/base.hpp>
#include <components/serialization/traits.hpp>
#include <core/pmr.hpp>

#include <boost/core/ignore_unused.hpp>
#include <map>
#include <vector>

namespace components::serialization {

    template<class Storage>
    void serialize_array(stream::stream<Storage>& ar, std::size_t size, const unsigned int version) {
        intermediate_serialize_array(ar, size, version);
    }

    template<class Storage>
    void serialize_array(stream::stream<Storage>& ar, std::size_t size) {
        serialize_array(ar, size, 0);
    }

    template<class Storage>
    void serialize_map(stream::stream<Storage>& ar, std::size_t size, const unsigned int version) {
        intermediate_serialize_map(ar, size, version);
    }

    template<class Storage>
    void serialize_map(stream::stream<Storage>& ar, std::size_t size) {
        serialize_map(ar, size, 0);
    }
/*
    template<class Storage,class C>
    void serialize(stream::stream<Storage>& ar, C&&data, const unsigned int version) {
        intermediate_serialize(ar, std::forward(data), version,serialization_trait<C>::tag());
    }

    template<class Storage,class C>
    void serialize(stream::stream<Storage>& ar,C&&data) {
        serialize(ar, std::forward(data), 0);
    }

    template<class Storage,class C>
    void serialize(stream::stream<Storage>& ar,std::string_view key, C&&data, const unsigned int version) {
        intermediate_serialize(ar,key, std::forward(data), version,serialization_trait<C>::tag());
    }

    template<class Storage,class C>
    void serialize(stream::stream<Storage>& ar,std::string_view key,C&&data) {
        serialize(ar,key, std::forward(data), 0);
    }

*/

} // namespace components::serialization