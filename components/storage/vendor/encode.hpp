#pragma once

#include <iostream>

namespace base64
{

extern "C"
{
#include "cencode.hpp"
}

class encoder_t
{
public:
    encoder_t() {
        base64_init_encodestate(&_state);
    }

    void set_chars_per_line(int chars_per_line) {
        _state.chars_per_line = chars_per_line;
    }

    int encode(char value_in) {
        return base64_encode_value(value_in);
    }

    size_t encode(const void* code_in, const size_t len_in, void* plaintext_out) {
        return base64_encode_block((const uint8_t*)code_in, len_in, plaintext_out, &_state);
    }

    size_t encode_end(char* plaintext_out) {
        return base64_encode_blockend(plaintext_out, &_state);
    }

    void encode(std::istream& istream_in, std::ostream& ostream_in, size_t buffer_size = 1024) {
        const size_t n = buffer_size;
        char* plaintext = new char[n];
        char* code = new char[2*n];
        std::streamsize plain_len;
        size_t code_len;
        do {
            istream_in.read(plaintext, n);
            plain_len = istream_in.gcount();
            code_len = encode(plaintext, plain_len, code);
            ostream_in.write(code, code_len);
        } while (istream_in.good() && plain_len > 0);
        code_len = encode_end(code);
        ostream_in.write(code, code_len);
        base64_init_encodestate(&_state);
        delete [] code;
        delete [] plaintext;
    }

private:
    base64_encode_state _state;
};

}
