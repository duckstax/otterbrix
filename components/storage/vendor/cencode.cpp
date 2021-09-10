#include "cencode.hpp"

const int default_chars_per_line = 72;

void base64_init_encodestate(base64_encode_state* state_in) {
    state_in->step = base64_encode_step::a;
    state_in->result = 0;
    state_in->stepcount = 0;
    state_in->chars_per_line = default_chars_per_line;
}

char base64_encode_value(uint8_t value_in) {
    static const char* encoding = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    if (value_in > 63) return '=';
    return encoding[(int)value_in];
}

size_t base64_encode_block(const uint8_t* plaintext_in, size_t length_in, void* code_out, base64_encode_state* state_in) {
    const uint8_t* plainchar = plaintext_in;
    const uint8_t* const plaintextend = plaintext_in + length_in;
    char* codechar = (char*)code_out;
    char result;
    char fragment;
    result = state_in->result;
    switch (state_in->step) {
    while (1) {
    case base64_encode_step::a:
            if (plainchar == plaintextend) {
                state_in->result = result;
                state_in->step = base64_encode_step::a;
                return codechar - (char*)code_out;
            }
            fragment = *plainchar++;
            result = (fragment & 0x0fc) >> 2;
            *codechar++ = base64_encode_value(result);
            result = (char)((fragment & 0x003) << 4);
        case base64_encode_step::b:
            if (plainchar == plaintextend) {
                state_in->result = result;
                state_in->step = base64_encode_step::b;
                return codechar - (char*)code_out;
            }
            fragment = *plainchar++;
            result |= (fragment & 0x0f0) >> 4;
            *codechar++ = base64_encode_value(result);
            result = (char)((fragment & 0x00f) << 2);
        case base64_encode_step::c:
            if (plainchar == plaintextend) {
                state_in->result = result;
                state_in->step = base64_encode_step::c;
                return codechar - (char*)code_out;
            }
            fragment = *plainchar++;
            result |= (fragment & 0x0c0) >> 6;
            *codechar++ = base64_encode_value(result);
            result  = (fragment & 0x03f) >> 0;
            *codechar++ = base64_encode_value(result);
            if (state_in->chars_per_line > 0) {
                ++(state_in->stepcount);
                if (state_in->stepcount == state_in->chars_per_line/4) {
                    *codechar++ = '\n';
                    state_in->stepcount = 0;
                }
            }
    }
    }
    return codechar - (char*)code_out;
}

size_t base64_encode_blockend(void* code_out, base64_encode_state* state_in) {
    char* codechar = (char*)code_out;
    switch (state_in->step)
    {
    case base64_encode_step::b:
        *codechar++ = base64_encode_value(state_in->result);
        *codechar++ = '=';
        *codechar++ = '=';
        break;
    case base64_encode_step::c:
        *codechar++ = base64_encode_value(state_in->result);
        *codechar++ = '=';
        break;
    case base64_encode_step::a:
        break;
    }
    if (state_in->chars_per_line > 0) {
        *codechar++ = '\n';
    }

    return codechar - (char*)code_out;
}
