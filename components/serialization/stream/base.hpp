#pragma once
#include <map>
#include <string>
#include <vector>

namespace components::serialization::stream {

    template<class T>
    class stream;
    /// anonymous
    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, int8_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, uint8_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, int16_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, uint16_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, int32_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, uint32_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, int64_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, uint64_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, const std::string& data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view data, const unsigned int version);

    template<class Storage, class T>
    void intermediate_serialize(stream<Storage>& ar, const std::vector<T>& data, const unsigned int version);

    template<class Storage, class K, class V>
    void intermediate_serialize(stream<Storage>& ar, const std::map<K, V>& data, const unsigned int version);
    /// anonym


    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, int8_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, uint8_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, int16_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, uint16_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, int32_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, uint32_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, int64_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, uint64_t data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, const std::string& data, const unsigned int version);

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, std::string_view data, const unsigned int version);

    template<class Storage, class T>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, const std::vector<T>& data, const unsigned int version);

    template<class Storage, class K, class V>
    void intermediate_serialize(stream<Storage>& ar, std::string_view key, const std::map<K, V>& data, const unsigned int version);

} // namespace components::serialization::stream