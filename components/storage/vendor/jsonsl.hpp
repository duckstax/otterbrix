#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <sys/types.h>
#include <wchar.h>

extern "C" {

#ifdef JSONSL_USE_WCHAR
typedef jsonsl_char_t wchar_t;
typedef jsonsl_uchar_t unsigned wchar_t;
#else
typedef char jsonsl_char_t;
typedef unsigned char jsonsl_uchar_t;
#endif

#ifdef JSONSL_PARSE_NAN
#define JSONSL__NAN_PROXY JSONSL_SPECIALf_NAN
#define JSONSL__INF_PROXY JSONSL_SPECIALf_INF
#else
#define JSONSL__NAN_PROXY 0
#define JSONSL__INF_PROXY 0
#endif

#if defined(_WIN32) && !defined(__MINGW32__) && (!defined(_MSC_VER) || _MSC_VER<1600)
typedef __int8 int8_t;
typedef unsigned __int8 uint8_t;
typedef __int16 int16_t;
typedef unsigned __int16 uint16_t;
typedef __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
#if !defined(_MSC_VER) || _MSC_VER<1400
typedef unsigned int size_t;
typedef int ssize_t;
#endif
#else
#include <stdint.h>
#endif

#if (!defined(JSONSL_STATE_GENERIC)) && (!defined(JSONSL_STATE_USER_FIELDS))
#define JSONSL_STATE_GENERIC
#endif

#ifdef JSONSL_STATE_GENERIC
#define JSONSL_STATE_USER_FIELDS
#endif

#ifndef JSONSL_JPR_COMPONENT_USER_FIELDS
#define JSONSL_JPR_COMPONENT_USER_FIELDS
#endif

#ifndef JSONSL_API
#if defined(_WIN32) && defined(JSONSL_DLL)
#define JSONSL_API __declspec(dllexport)
#else
#define JSONSL_API
#endif
#endif

#ifndef JSONSL_INLINE
#if defined(_MSC_VER)
  #define JSONSL_INLINE __inline
  #elif defined(__GNUC__)
  #define JSONSL_INLINE __inline__
  #else
  #define JSONSL_INLINE inline
  #endif
#endif

#define JSONSL_MAX_LEVELS 512

struct jsonsl_st;
typedef struct jsonsl_st *jsonsl_t;
typedef struct jsonsl_jpr_st* jsonsl_jpr_t;

#pragma once

#define JSONSL_Tf_STRINGY 0xffff00

#define JSONSL_XTYPE \
    X(STRING,   '"'|JSONSL_Tf_STRINGY) \
    X(HKEY,     '#'|JSONSL_Tf_STRINGY) \
    X(OBJECT,   '{') \
    X(LIST,     '[') \
    X(SPECIAL,  '^') \
    X(UESCAPE,  'u')

enum jsonsl_type_t {
#define X(o, c) \
    JSONSL_T_##o = c,
    JSONSL_XTYPE
    JSONSL_T_UNKNOWN = '?',
    JSONSL_T_ROOT = 0
#undef X
};

#define JSONSL_XSPECIAL \
    X(NONE, 0) \
    X(SIGNED,       1<<0) \
    X(UNSIGNED,     1<<1) \
    X(TRUE,         1<<2) \
    X(FALSE,        1<<3) \
    X(NULL,         1<<4) \
    X(FLOAT,        1<<5) \
    X(EXPONENT,     1<<6) \
    X(NONASCII,     1<<7) \
    X(NAN,          1<<8) \
    X(INF,          1<<9)

enum  jsonsl_special_t{
#define X(o,b) \
    JSONSL_SPECIALf_##o = b,
    JSONSL_XSPECIAL
#undef X
    JSONSL_SPECIALf_UNKNOWN = 1 << 10,
    JSONSL_SPECIALf_ZERO    = 1 << 11 | JSONSL_SPECIALf_UNSIGNED,
    JSONSL_SPECIALf_DASH    = 1 << 12,
    JSONSL_SPECIALf_POS_INF = (JSONSL_SPECIALf_INF),
    JSONSL_SPECIALf_NEG_INF = (JSONSL_SPECIALf_INF|JSONSL_SPECIALf_SIGNED),
    JSONSL_SPECIALf_NUMERIC = (JSONSL_SPECIALf_SIGNED| JSONSL_SPECIALf_UNSIGNED),
    JSONSL_SPECIALf_BOOLEAN = (JSONSL_SPECIALf_TRUE|JSONSL_SPECIALf_FALSE),
   JSONSL_SPECIALf_NUMNOINT =
       (JSONSL_SPECIALf_FLOAT|JSONSL_SPECIALf_EXPONENT|JSONSL_SPECIALf_NAN
        |JSONSL_SPECIALf_INF)
};

#define JSONSL_XACTION \
    X(PUSH, '+') \
    X(POP, '-') \
    X(UESCAPE, 'U') \
    X(ERROR, '!')

enum jsonsl_action_t {
#define X(a,c) \
    JSONSL_ACTION_##a = c,
    JSONSL_XACTION
    JSONSL_ACTION_UNKNOWN = '?'
#undef X
};

#define JSONSL_XERR \
    X(GARBAGE_TRAILING) \
    X(SPECIAL_EXPECTED) \
    X(SPECIAL_INCOMPLETE) \
    X(STRAY_TOKEN) \
    X(MISSING_TOKEN) \
    X(CANT_INSERT) \
    X(ESCAPE_OUTSIDE_STRING) \
    X(KEY_OUTSIDE_OBJECT) \
    X(STRING_OUTSIDE_CONTAINER) \
    X(FOUND_NULL_BYTE) \
    X(LEVELS_EXCEEDED) \
    X(BRACKET_MISMATCH) \
    X(HKEY_EXPECTED) \
    X(WEIRD_WHITESPACE) \
    X(UESCAPE_TOOSHORT) \
    X(ESCAPE_INVALID) \
    X(TRAILING_COMMA) \
    X(INVALID_NUMBER) \
    X(VALUE_EXPECTED) \
    X(PERCENT_BADHEX) \
    X(JPR_BADPATH) \
    X(JPR_DUPSLASH) \
    X(JPR_NOROOT) \
    X(ENOMEM) \
    X(INVALID_CODEPOINT)

enum jsonsl_error_t {
    JSONSL_ERROR_SUCCESS = 0,
#define X(e) \
    JSONSL_ERROR_##e,
    JSONSL_XERR
#undef X
    JSONSL_ERROR_GENERIC
};


struct jsonsl_state_st {
    unsigned type;
    unsigned special_flags;
    size_t pos_begin;
    size_t pos_cur;
    unsigned int level;
    uint64_t nelem;
    int ignore_callback;
    unsigned int nescapes;
#ifndef JSONSL_STATE_GENERIC
    JSONSL_STATE_USER_FIELDS
#else
    void *data;
#endif
};

#define JSONSL_LIST_SIZE(st) ((st)->nelem)
#define JSONSL_OBJECT_SIZE(st) ((st)->nelem / 2)
#define JSONSL_NUMERIC_VALUE(st) ((st)->nelem)

typedef void (*jsonsl_stack_callback)(
        jsonsl_t jsn,
        jsonsl_action_t action,
        struct jsonsl_state_st* state,
        const jsonsl_char_t *at);

typedef int (*jsonsl_error_callback)(
        jsonsl_t jsn,
        jsonsl_error_t error,
        struct jsonsl_state_st* state,
        jsonsl_char_t *at);

struct jsonsl_st {
    unsigned int level;
    unsigned int stopfl;
    size_t pos;
    const jsonsl_char_t *base;
    jsonsl_stack_callback action_callback_PUSH;
    jsonsl_stack_callback action_callback_POP;
    jsonsl_stack_callback action_callback;
    unsigned int max_callback_level;
    jsonsl_error_callback error_callback;

    int call_SPECIAL;
    int call_OBJECT;
    int call_LIST;
    int call_STRING;
    int call_HKEY;

    jsonsl_stack_callback action_callback_UESCAPE;
    int call_UESCAPE;
    int return_UESCAPE;

    struct {
        int allow_trailing_comma;
    } options;

    void *data;
    int in_escape;
    char expecting;
    char tok_last;
    int can_insert;
    unsigned int levels_max;
#ifndef JSONSL_NO_JPR
    size_t jpr_count;
    jsonsl_jpr_t *jprs;
    size_t *jpr_root;
#endif
    struct jsonsl_state_st stack[1];
};


JSONSL_API jsonsl_t jsonsl_new(int nlevels);
JSONSL_API void jsonsl_feed(jsonsl_t jsn, const jsonsl_char_t *bytes, size_t nbytes);
JSONSL_API void jsonsl_reset(jsonsl_t jsn);
JSONSL_API void jsonsl_destroy(jsonsl_t jsn);

static JSONSL_INLINE struct jsonsl_state_st *jsonsl_last_state(const jsonsl_t jsn, const struct jsonsl_state_st *state) {
    if (state->level > 1) {
        return jsn->stack + state->level - 1;
    } else {
        return NULL;
    }
}

static JSONSL_INLINE struct jsonsl_state_st *jsonsl_last_child(const jsonsl_t jsn, const struct jsonsl_state_st *parent) {
    return jsn->stack + (parent->level + 1);
}

static JSONSL_INLINE void jsonsl_stop(jsonsl_t jsn) {
    jsn->stopfl = 1;
}

static JSONSL_INLINE void jsonsl_enable_all_callbacks(jsonsl_t jsn) {
    jsn->call_HKEY = 1;
    jsn->call_STRING = 1;
    jsn->call_OBJECT = 1;
    jsn->call_SPECIAL = 1;
    jsn->call_LIST = 1;
}

#define JSONSL_STATE_IS_CONTAINER(state) (state->type == JSONSL_T_OBJECT || state->type == JSONSL_T_LIST)

JSONSL_API const char* jsonsl_strerror(jsonsl_error_t err);
JSONSL_API const char* jsonsl_strtype(jsonsl_type_t jt);
JSONSL_API void jsonsl_dump_global_metrics(void);

#ifndef JSONSL_NO_JPR
#ifndef JSONSL_PATH_WILDCARD_CHAR
#define JSONSL_PATH_WILDCARD_CHAR '^'
#endif

#define JSONSL_XMATCH \
    X(COMPLETE,1) \
    X(POSSIBLE,0) \
    X(NOMATCH,-1) \
    X(TYPE_MISMATCH, -2)

enum jsonsl_jpr_match_t {
#define X(T,v) \
    JSONSL_MATCH_##T = v,
    JSONSL_XMATCH
#undef X
    JSONSL_MATCH_UNKNOWN
};

enum jsonsl_jpr_type_t {
    JSONSL_PATH_STRING = 1,
    JSONSL_PATH_WILDCARD,
    JSONSL_PATH_NUMERIC,
    JSONSL_PATH_ROOT,
    JSONSL_PATH_INVALID = -1,
    JSONSL_PATH_NONE = 0
};


struct jsonsl_jpr_component_st {
    char *pstr;
    unsigned long idx;
    size_t len;
    jsonsl_jpr_type_t ptype;
    short is_arridx;
    JSONSL_JPR_COMPONENT_USER_FIELDS
};


struct jsonsl_jpr_st {
    struct jsonsl_jpr_component_st *components;
    size_t ncomponents;
    unsigned match_type;
    char *basestr;
    char *orig;
    size_t norig;
};

JSONSL_API jsonsl_jpr_t jsonsl_jpr_new(const char *path, jsonsl_error_t *errp);
JSONSL_API void jsonsl_jpr_destroy(jsonsl_jpr_t jpr);
JSONSL_API jsonsl_jpr_match_t jsonsl_jpr_match(jsonsl_jpr_t jpr, unsigned int parent_type, unsigned int parent_level, const char *key, size_t nkey);
JSONSL_API jsonsl_jpr_match_t jsonsl_path_match(jsonsl_jpr_t jpr, const struct jsonsl_state_st *parent, const struct jsonsl_state_st *child, const char *key, size_t nkey);
JSONSL_API void jsonsl_jpr_match_state_init(jsonsl_t jsn, jsonsl_jpr_t *jprs, size_t njprs);
JSONSL_API jsonsl_jpr_t jsonsl_jpr_match_state(jsonsl_t jsn, struct jsonsl_state_st *state, const char *key, size_t nkey, jsonsl_jpr_match_t *out);
JSONSL_API void jsonsl_jpr_match_state_cleanup(jsonsl_t jsn);
JSONSL_API const char *jsonsl_strmatchtype(jsonsl_jpr_match_t match);
JSONSL_API size_t jsonsl_util_unescape_ex(const char *in, char *out, size_t len, const int toEscape[128], unsigned *oflags, jsonsl_error_t *err, const char **errat);

#define jsonsl_util_unescape(in, out, len, toEscape, err) \
    jsonsl_util_unescape_ex(in, out, len, toEscape, NULL, err, NULL)

#endif

}
