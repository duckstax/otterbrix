#pragma once

#include <iosfwd>
#include <stdexcept>
#include <string>

namespace storage {

void convert_json5(std::istream &in, std::ostream &out);
std::string convert_json5(const std::string &in);


class json5_error : public std::runtime_error {
public:
    json5_error(const std::string &what, std::string::size_type input_pos);

    std::string::size_type const input_pos;
};

}
