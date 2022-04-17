#pragma once
#include <cstdint>

bool is_hex(char c);
void char_to_hex(uint8_t c, char *hex);
void hex_to_char(const char *hex, uint8_t &c);
