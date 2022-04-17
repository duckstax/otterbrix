#include "hex.hpp"

char to_char(uint8_t value) {
    if (value < 10) {
        return char('0' + value);
    }
    return char('a' + value - 10);
}

uint8_t to_int(char c) {
    if (c >= '0' && c <= '9') {
        return uint8_t(c - '0');
    }
    if (c >= 'a' && c <= 'f') {
        return uint8_t(c + 10 - 'a');
    }
    return uint8_t(c + 10 - 'A');
}

bool is_hex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

void char_to_hex(uint8_t c, char *hex) {
    hex[0] = to_char(c / 0x10);
    hex[1] = to_char(c % 0x10);
}

void hex_to_char(const char *hex, uint8_t &c) {
    c = to_int(hex[0]) * 0x10 + to_int(hex[1]);
}
