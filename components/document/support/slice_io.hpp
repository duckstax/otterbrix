#pragma once

#include <components/document/core/slice.hpp>
#include <string>
#include <stdio.h>

#ifndef FL_HAVE_FILESYSTEM
#define FL_HAVE_FILESYSTEM 1
#endif

#if FL_HAVE_FILESYSTEM

namespace document {

alloc_slice_t read_file(const char *path);

void write_to_file(slice_t s, const char *path, int mode);
void write_to_file(slice_t s, const char *path);
void append_to_file(slice_t s, const char *path);

}

#endif

