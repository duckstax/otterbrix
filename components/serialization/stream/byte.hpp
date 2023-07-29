#pragma once
#include "base.hpp"

#include "byte_container.hpp"

namespace components::serialization::stream {


    template<>
    class output_stream<byte_container_t> {
    public:
        output_stream() = default;
        ~output_stream() = default;

        byte_container_t value_;
    };

    using stream_byte = output_stream<byte_container_t>;

}