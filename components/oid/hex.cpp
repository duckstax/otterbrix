#include "hex.hpp"

char to_char(unsigned char value) {
    if (value < 10) {
        return char('0' + value);
    }
    return char('a' + value - 10);
}

int to_int(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c + 10 - 'a';
    }
    return c + 10 - 'A';
}

bool is_hex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

void char_to_hex(char c, char *hex) {
    hex[0] = to_char(static_cast<unsigned char>(c) / 0x10);
    hex[1] = to_char(static_cast<unsigned char>(c) % 0x10);
}

void hex_to_char(const char *hex, char &c) {
    c = char(to_int(hex[0]) * 0x10 + to_int(hex[1]));
}
