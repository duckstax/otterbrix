#pragma once

#include <cstdlib>
#include <string>

#include "nodes/parsenodes.h"

#include <stdexcept>

class parser_exception_t : public std::exception {
public:
    parser_exception_t(int main_code, int support_code, std::string message, std::string detail, int pos);
    parser_exception_t(std::string message, std::string detail);

    const char* what() const noexcept override;

    int main_error_code = -1;
    int support_error_code = -1;
    std::string message;
    std::string detail;
    int query_pos = -1;
};

// error handling
int ereport(int code, std::string message);
int ereport(int code, std::string message, int pos);
int ereport(int main_code, int support_code, std::string message);
int ereport(int main_code, int support_code, std::string message, int pos);
int ereport(int main_code, int support_code, std::string message, std::string detail);
int ereport(int main_code, int support_code, std::string message, std::string detail, int pos);

void elog(int code, const char* message, ...);
int errcode(int sqlerrcode);

// TODO: use std::format (C++ 20)
template<typename... Args>
std::string errmsg(const char* fmt, Args... args) {
    int size_s = std::snprintf(nullptr, 0, fmt, args...) + 1; // Extra space for '\0'
    if (size_s <= 0) {
        throw std::runtime_error("Error during formatting.");
    }
    auto size = static_cast<size_t>(size_s);
    std::string result;
    result.resize(size);
    std::snprintf(result.data(), size, fmt, args...);
    result.resize(size - 1); // remove '\0'
    return result;
}
const char* errhint(const char* msg);
const char* errmsg_internal(const char* fmt, ...);
const char* errdetail(const char* fmt, ...);
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
