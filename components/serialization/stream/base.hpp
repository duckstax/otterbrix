#pragma once
#include <string>
#include <map>
#include <vector>

namespace components::serialization::stream{

    template<class T>
    class stream ;

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, uint64_t& data, const unsigned int version) ;

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, const std::string& data, const unsigned int version) ;

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string_view data, const unsigned int version) ;

    template<class Storage,class T>
    void intermediate_serialize(stream<Storage>& ar, const std::vector<T>& data, const unsigned int version);

    template<class Storage,class K, class V>
    void intermediate_serialize(stream<Storage>& ar, const std::map<K, V>& data, const unsigned int version);

}