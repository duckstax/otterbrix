#pragma once
#include "nodes/parsenodes.h"

/* Primary entry point for the raw parsing functions */
extern List* raw_parser(const char* str);

extern List* raw_parser_copy_options(const char* str);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List* SystemFuncName(char* name);
extern TypeName* SystemTypeName(char* name);