#pragma once
#include "base.hpp"

#include "byte_container.hpp"

namespace components::serialization::stream {


    template<>
    class stream<byte_container_t> {
    public:
        stream() = default;
        ~stream() = default;

        byte_container_t value_;
    };

    using stream_byte = stream<byte_container_t>;

}