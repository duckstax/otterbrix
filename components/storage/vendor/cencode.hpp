#pragma once

#include <stdint.h>
#include <stdlib.h>

extern "C" {

enum class base64_encode_step { a, b, c };

struct base64_encode_state {
    base64_encode_step step;
    char result;
    int stepcount;
    int chars_per_line;
};

void base64_init_encodestate(base64_encode_state* state_in);
char base64_encode_value(uint8_t value_in);
size_t base64_encode_block(const uint8_t* plaintext_in, size_t length_in, void* code_out, base64_encode_state* state_in);
size_t base64_encode_blockend(void* code_out, base64_encode_state* state_in);

}
