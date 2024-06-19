#pragma once

#include <cassert>
#include <cfloat>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#ifndef _WIN32
// strcasecmp, strncasecmp
#include <strings.h>
#endif

#define _usually_true(VAL) __builtin_expect(VAL, true)
#define _usually_false(VAL) __builtin_expect(VAL, false)