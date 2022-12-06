#include <cstdint>

#include <string>

struct value_type_traits final  {
    using boolean_t = bool;
    using number_float_t = float;
    using number_double_t = double;
    using number_integer_t = std::int64_t;
    using number_unsigned_t = std::uint64_t;
    using string_t = std::string;
    using object_t;
    using array_t;
    using binary_t;
};