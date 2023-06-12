#pragma once
#include <string>
#include <map>
#include <vector>

namespace components::serialization::stream{

    template<class T>
    class stream ;

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, uint64_t& t, const unsigned int version) ;

    template<class Storage>
    void intermediate_serialize(stream<Storage>& ar, std::string& t, const unsigned int version) ;

    template<class Storage,class T>
    void intermediate_serialize(stream<Storage>& ar, std::vector<T>& t, const unsigned int version);

    template<class Storage,class K, class V>
    void intermediate_serialize(stream<Storage>& ar, std::map<K, V>& t, const unsigned int version);

}