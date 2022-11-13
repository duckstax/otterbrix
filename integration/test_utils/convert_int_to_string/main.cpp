#include <iostream>
#include <magic_enum.hpp>
#include <services/wal/route.hpp>

constexpr uint64_t unpack_handler_id(group_id_t group, uint64_t input) {
    return  input - 100 * static_cast<uint64_t>(group);
}

auto convert_int_to_string(group_id_t group,uint64_t number){
    auto color = magic_enum::enum_cast<services::wal::route>(unpack_handler_id(group,number));
    if (color.has_value()) {
        auto color_name = magic_enum::enum_name(color.value());
        return color_name;
    }
    return std::string_view("Y_ii_Y");
}

int main() {

    constexpr int control_number = 1152921504606847388;
    std::cerr << convert_int_to_string(group_id_t::wal,control_number) << std::endl;
    return 0;
}