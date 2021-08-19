#include "../protocol/select.hpp"
#include "../protocol/protocol.hpp"
#include <iostream>
#include <msgpack.hpp>

int main() {
    protocol_t protocol_("1qaz-2wsx-3edc",protocol_op::select,select_t("test", std::vector<std::string>{"key1"}));
    msgpack::zone z;
    msgpack::object obj(protocol_, z);
    std::cout << obj << std::endl;
    auto d = obj.as<protocol_t>();
    return 0;
}