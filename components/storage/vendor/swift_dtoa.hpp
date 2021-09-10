#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

extern "C" {
namespace storage {

int swift_decompose_double(double d, int8_t *digits, size_t digits_length, int *decimalExponent);
size_t swift_format_double(double, char *dest, size_t length);
int swift_decompose_float(float f, int8_t *digits, size_t digits_length, int *decimalExponent);
size_t swift_format_float(float, char *dest, size_t length);
int swift_decompose_float80(long double f, int8_t *digits, size_t digits_length, int *decimalExponent);
size_t swift_format_float80(long double, char *dest, size_t length);
size_t swift_format_exponential(char *dest, size_t length, bool negative, const int8_t *digits, int digits_count, int decimalExponent);
size_t swift_format_decimal(char *dest, size_t length, bool negative, const int8_t *digits, int digits_count, int decimalExponent);

}
}
