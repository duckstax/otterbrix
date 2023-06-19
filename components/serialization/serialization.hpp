#pragma once

#include <core/pmr.hpp>
#include <components/serialization/stream.hpp>
#include <components/serialization/traits.hpp>

#include <map>
#include <vector>

namespace components::serialization {

    template<class Storage>
    void serialize_array(stream::stream<Storage>& ar, std::size_t size, const unsigned int version) {

    }

    template<class Storage>
    void serialize_map(stream::stream<Storage>& ar, std::size_t size, const unsigned int version) {

    }


    template<class Storage, class C>
    void serialize_array(stream::stream<Storage>& ar, C& t, const unsigned int version) {

    }

    template<class Storage, class C>
    void serialize_map(stream::stream<Storage>& ar, C& t, const unsigned int version) {

    }

    template<class Storage>
    void serialize_string(stream::stream<Storage>& ar, const std::string& t, const unsigned int version) {

    }

    template<class Storage>
    void serialize_string(stream::stream<Storage>& ar, std::string_view t, const unsigned int version) {

    }

    template<class Storage, class C>
    void serialize_blob(stream::stream<Storage>& ar, C& t, const unsigned int version) {

    }

    template<class Storage, class C>
    void serialize_extension(stream::stream<Storage>& ar, C& t, const unsigned int version) {

    }

} // namespace components::serialization