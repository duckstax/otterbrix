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

    ///####### anonyms serialize
    template<class Storage>
    void serialize(stream::stream<Storage>& ar, bool data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, bool data) {
        serialize(ar, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int8_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int8_t data) {
        serialize(ar, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int16_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int16_t data) {
        serialize(ar, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int32_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int32_t data) {
        serialize(ar, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int64_t data, const unsigned int version) {
        intermediate_serialize(ar, data, version);
    }
    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int64_t data) {
        serialize(ar, data, 0);
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

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::uint64_t data) {
        serialize(ar, data, 0);
    }

    namespace detail{
        ///template<class Storage,class Contaner>
        ///void serialize_help(stream::stream<Storage>& ar, const Contaner& data, const unsigned int version){
        ///    intermediate_serialize(ar,std::size(data),std::begin(data),std::end(data),version,serialization_trait<Contaner>::category);
        ///}
    }

    template<class Storage,class Contaner>
    void serialize(stream::stream<Storage>& ar, const Contaner& data, const unsigned int version) {
        intermediate_serialize(ar,std::size(data),std::begin(data),std::end(data),version, typename serialization_trait<Contaner>::category{});
    }

    template<class Storage,class Contaner>
    void serialize(stream::stream<Storage>& ar, const Contaner& data) {
        serialize(ar,data,0);
    }

    ///####### anonyms serialize

    ///####### name serialize
    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, bool data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, bool data) {
        serialize(ar, key, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::int8_t data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::int8_t data) {
        serialize(ar, key, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::int16_t data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::int16_t data) {
        serialize(ar, key, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::int32_t data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::int32_t data) {
        serialize(ar, key, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::int64_t data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }
    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::int64_t data) {
        serialize(ar, key, data, 0);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::uint8_t data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::uint16_t data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::uint32_t data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::uint64_t data, const unsigned int version) {
        intermediate_serialize(ar, key, data, version);
    }

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::string_view key, std::uint64_t data) {
        serialize(ar, key, data, 0);
    }
    ///####### name serialize
} // namespace components::serialization