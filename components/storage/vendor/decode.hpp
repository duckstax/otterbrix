#pragma once

#include <iostream>

namespace base64
{

extern "C"
{
#include "cdecode.hpp"
}

class decoder_t
{
public:
    decoder_t() {
        base64_init_decodestate(&_state);
    }

    int decode(char value_in) {
        return base64_decode_value(value_in);
    }

    size_t decode(const void* code_in, const size_t len_in, void* plaintext_out) {
        return base64_decode_block((const uint8_t*)code_in, len_in, plaintext_out, &_state);
    }

    void decode(std::istream& istream_in, std::ostream& ostream_in, int buffer_size_in = 1024) {
        base64_init_decodestate(&_state);
        const int n = buffer_size_in;
        char* code = new char[n];
        char* plaintext = new char[n];
        std::streamsize code_len;
        size_t plain_len;
        do {
            istream_in.read((char*)code, n);
            code_len = istream_in.gcount();
            plain_len = decode(code, code_len, plaintext);
            ostream_in.write((const char*)plaintext, plain_len);
        } while (istream_in.good() && code_len > 0);
        base64_init_decodestate(&_state);
        delete [] code;
        delete [] plaintext;
    }

private:
    base64_decode_state _state;
};

}
