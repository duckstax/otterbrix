#pragma once

#include <components/serialization/stream.hpp>
#include <components/serialization/traits.hpp>
#include <core/pmr.hpp>

#include <map>
#include <vector>

namespace components::serialization {

    template<class Storage>
    void serialize_array(stream::stream<Storage>& ar, std::size_t size, const unsigned int version) {
        intermediate_serialize_array(ar,size, version);
    }

    template<class Storage>
    void serialize_array(stream::stream<Storage>& ar, std::size_t size) {
        serialize_array(ar, size, 0);
    }

    template<class Storage>
    void serialize_map(stream::stream<Storage>& ar, std::size_t size, const unsigned int version) {
        intermediate_serialize_map(ar,size, version);
    }

    template<class Storage>
    void serialize_map(stream::stream<Storage>& ar, std::size_t size) {
        serialize_map(ar, size, 0);
    }
/*
    template<class Storage, class C>
    void serialize_blob(stream::stream<Storage>& ar, C& data, const unsigned int version) {
    }

    template<class Storage, class C>
    void serialize_extension(stream::stream<Storage>& ar, C& data, const unsigned int version) {
    }
    */

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int8_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int16_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int32_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int64_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::uint8_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::uint16_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::uint32_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::uint64_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage, class T>
    void serialize(stream::stream<Storage>& ar, std::vector<T>& data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage, class T>
    void serialize(stream::stream<Storage>& ar, std::vector<T>& data) {
        serialize(ar, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, const std::string& data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, const std::string& data) {
        serialize(ar, data, 0);
    }
/*
    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view data,const unsigned int version) {

    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view data) {
        serialize(ar, data, 0);
    }

    template<class Storage, class Key, class Value>
    void serialize(stream::stream<Storage>& ar, std::map<Key, Value>& data, const unsigned int version) {
    }*/

} // namespace components::serialization