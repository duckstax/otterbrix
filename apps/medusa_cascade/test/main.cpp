#include "../protocol/select.hpp"
#include <iostream>
#include <msgpack.hpp>

int main() {
    select_t select("test", std::vector<std::string>{"key1"});
    msgpack::zone z;
    msgpack::object obj(select, z);
    std::cout << obj << std::endl;
    auto d = obj.as<select_t>();
    return 0;
}