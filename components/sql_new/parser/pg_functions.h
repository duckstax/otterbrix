#pragma once

#include <cstdlib>
#include <string>

#include "nodes/parsenodes.h"

// error handling
int ereport(int code, ...);

void elog(int code, const char* fmt, ...);
int errcode(int sqlerrcode);
int errmsg(const char* fmt, ...);
int errhint(const char* msg);
int errmsg_internal(const char* fmt, ...);
int errdetail(const char* fmt, ...);
int errposition(int cursorpos);
char* psprintf(const char* fmt, ...);

// memory mgmt
char* pstrdup(const char* in);
void* palloc(size_t n);
void pfree(void* ptr);
void* palloc0fast(size_t n);
void* repalloc(void* ptr, size_t n);

std::string NameListToString(PGList* names); // mdxn: used only in ereport
int exprLocation(const Node* expr);          // nodefuncs

// string gunk
// mdxn: pg_wchar.c
bool pg_verifymbstr(const char* mbstr, int len, bool noError);
int pg_mbstrlen_with_len(const char* mbstr, int len);
int pg_mblen(const char* mbstr);

DefElem* defWithOids(bool value);

typedef unsigned int pg_wchar;
unsigned char* unicode_to_utf8(pg_wchar c, unsigned char* utf8string);
