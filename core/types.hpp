#pragma once

#include <string>

namespace core {
    enum class type
    {
        undef,
        str,
        int8,
        int16,
        int32,
        int64,
        uint8,
        uint16,
        uint32,
        uint64,
        float32,
        float64,
        bool8
    };

    inline std::string type_name(type type) noexcept {
        switch (type) {
            case type::undef:
                return "undef";
            case type::str:
                return "srt";
            case type::int8:
                return "int8";
            case type::int16:
                return "int16";
            case type::int32:
                return "int32";
            case type::int64:
                return "int64";
            case type::uint8:
                return "uint8";
            case type::uint16:
                return "uint16";
            case type::uint32:
                return "uint32";
            case type::uint64:
                return "uint64";
            case type::float32:
                return "float32";
            case type::float64:
                return "float64";
            case type::bool8:
                return "bool8";
        }
        return "invalid";
    }
} // namespace core