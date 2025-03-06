#pragma once

extern bool gp_ignore_window_exclude;
extern bool gp_ignore_error_table;

typedef enum
{
    BACKSLASH_QUOTE_OFF,
    BACKSLASH_QUOTE_ON,
    BACKSLASH_QUOTE_SAFE_ENCODING
} BackslashQuoteType;

extern int backslash_quote;
extern bool escape_string_warning;
