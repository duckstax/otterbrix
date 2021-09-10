#pragma once

#include <stdint.h>
#include <stdlib.h>

extern "C" {

enum class base64_decode_step { a, b, c, d };

struct base64_decode_state {
    base64_decode_step step;
    uint8_t plainchar;
};

void base64_init_decodestate(base64_decode_state* state_in);
char base64_decode_value(uint8_t value_in);
size_t base64_decode_block(const uint8_t* code_in, const size_t length_in, void* plaintext_out, base64_decode_state* state_in);

}
