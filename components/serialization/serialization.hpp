#pragma once
#include <boost/container/pmr/memory_resource.hpp>
#include <components/serialization/stream.hpp>
#include <components/serialization/traits.hpp>

#include <map>
#include <vector>

namespace components::serialization {

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, uint64_t& t, const unsigned int version) {
        intermediate_serialize(ar,t,version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string& t, const unsigned int version) {
        intermediate_serialize(ar,t,version);
    }

    template<class Storage, class T>
    void serialize(stream::stream<Storage>& ar, std::vector<T>& t, const unsigned int version) {
        intermediate_serialize(ar,t,version);
    }

    template<class Storage, class K, class V>
    void serialize(stream::stream<Storage>& ar, std::map<K, V>& t, const unsigned int version) {
        intermediate_serialize(ar,t,version);
    }

} // namespace components::serialization