/*-------------------------------------------------------------------------
*
* pg_wchar.h
*	  multibyte-character support
*
* Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
* src/include/mb/pg_wchar.h
*
*	NOTES
*		This is used both by the backend and by libpq, but should not be
*		included by libpq client programs.  In particular, a libpq client
*		should not assume that the encoding IDs used by the version of libpq
*		it's linked to match up with the IDs declared here.
*
*-------------------------------------------------------------------------
*/
#ifndef PG_WCHAR_H
#define PG_WCHAR_H

#include "nodes/pg_type_definitions.h"

/*
* The pg_wchar type
*/
typedef unsigned int pg_wchar;

/*
* Maximum byte length of multibyte characters in any backend encoding
*/
#define MAX_MULTIBYTE_CHAR_LEN 4

/*
* various definitions for EUC
*/
#define SS2 0x8e /* single shift 2 (JIS0201) */
#define SS3 0x8f /* single shift 3 (JIS0212) */

/*
* SJIS validation macros
*/
#define ISSJISHEAD(c) (((c) >= 0x81 && (c) <= 0x9f) || ((c) >= 0xe0 && (c) <= 0xfc))
#define ISSJISTAIL(c) (((c) >= 0x40 && (c) <= 0x7e) || ((c) >= 0x80 && (c) <= 0xfc))

/*----------------------------------------------------
* MULE Internal Encoding (MIC)
*
* This encoding follows the design used within XEmacs; it is meant to
* subsume many externally-defined character sets.  Each character includes
* identification of the character set it belongs to, so the encoding is
* general but somewhat bulky.
*
* Currently PostgreSQL supports 5 types of MULE character sets:
*
* 1) 1-byte ASCII characters.  Each byte is below 0x80.
*
* 2) "Official" single byte charsets such as ISO-8859-1 (Latin1).
*	  Each MULE character consists of 2 bytes: LC1 + C1, where LC1 is
*	  an identifier for the charset (in the range 0x81 to 0x8d) and C1
*	  is the character code (in the range 0xa0 to 0xff).
*
* 3) "Private" single byte charsets such as SISHENG.  Each MULE
*	  character consists of 3 bytes: LCPRV1 + LC12 + C1, where LCPRV1
*	  is a private-charset flag, LC12 is an identifier for the charset,
*	  and C1 is the character code (in the range 0xa0 to 0xff).
*	  LCPRV1 is either 0x9a (if LC12 is in the range 0xa0 to 0xdf)
*	  or 0x9b (if LC12 is in the range 0xe0 to 0xef).
*
* 4) "Official" multibyte charsets such as JIS X0208.  Each MULE
*	  character consists of 3 bytes: LC2 + C1 + C2, where LC2 is
*	  an identifier for the charset (in the range 0x90 to 0x99) and C1
*	  and C2 form the character code (each in the range 0xa0 to 0xff).
*
* 5) "Private" multibyte charsets such as CNS 11643-1992 Plane 3.
*	  Each MULE character consists of 4 bytes: LCPRV2 + LC22 + C1 + C2,
*	  where LCPRV2 is a private-charset flag, LC22 is an identifier for
*	  the charset, and C1 and C2 form the character code (each in the range
*	  0xa0 to 0xff).  LCPRV2 is either 0x9c (if LC22 is in the range 0xf0
*	  to 0xf4) or 0x9d (if LC22 is in the range 0xf5 to 0xfe).
*
* "Official" encodings are those that have been assigned code numbers by
* the XEmacs project; "private" encodings have Postgres-specific charset
* identifiers.
*
* See the "XEmacs Internals Manual", available at http://www.xemacs.org,
* for more details.  Note that for historical reasons, Postgres'
* private-charset flag values do not match what XEmacs says they should be,
* so this isn't really exactly MULE (not that private charsets would be
* interoperable anyway).
*
* Note that XEmacs's implementation is different from what emacs does.
* We follow emacs's implementaion, rathter than XEmacs's.
*----------------------------------------------------
*/

/*
* Charset identifiers (also called "leading bytes" in the MULE documentation)
*/

/*
* Charset IDs for official single byte encodings (0x81-0x8e)
*/
#define LC_ISO8859_1 0x81 /* ISO8859 Latin 1 */
#define LC_ISO8859_2 0x82 /* ISO8859 Latin 2 */
#define LC_ISO8859_3 0x83 /* ISO8859 Latin 3 */
#define LC_ISO8859_4 0x84 /* ISO8859 Latin 4 */
#define LC_TIS620 0x85    /* Thai (not supported yet) */
#define LC_ISO8859_7 0x86 /* Greek (not supported yet) */
#define LC_ISO8859_6 0x87 /* Arabic (not supported yet) */
#define LC_ISO8859_8 0x88 /* Hebrew (not supported yet) */
#define LC_JISX0201K 0x89 /* Japanese 1 byte kana */
#define LC_JISX0201R 0x8a /* Japanese 1 byte Roman */
/* Note that 0x8b seems to be unused as of Emacs 20.7.
* However, there might be a chance that 0x8b could be used
* in later versions of Emacs.
*/
#define LC_KOI8_R 0x8b     /* Cyrillic KOI8-R */
#define LC_ISO8859_5 0x8c  /* ISO8859 Cyrillic */
#define LC_ISO8859_9 0x8d  /* ISO8859 Latin 5 (not supported yet) */
#define LC_ISO8859_15 0x8e /* ISO8859 Latin 15 (not supported yet) */
/* #define CONTROL_1		0x8f	control characters (unused) */

/* Is a leading byte for "official" single byte encodings? */
#define IS_LC1(c) ((unsigned char) (c) >= 0x81 && (unsigned char) (c) <= 0x8d)

/*
* Charset IDs for official multibyte encodings (0x90-0x99)
* 0x9a-0x9d are free. 0x9e and 0x9f are reserved.
*/
#define LC_JISX0208_1978 0x90 /* Japanese Kanji, old JIS (not supported) */
#define LC_GB2312_80 0x91     /* Chinese */
#define LC_JISX0208 0x92      /* Japanese Kanji (JIS X 0208) */
#define LC_KS5601 0x93        /* Korean */
#define LC_JISX0212 0x94      /* Japanese Kanji (JIS X 0212) */
#define LC_CNS11643_1 0x95    /* CNS 11643-1992 Plane 1 */
#define LC_CNS11643_2 0x96    /* CNS 11643-1992 Plane 2 */
#define LC_JISX0213_1                                                                                                  \
    0x97               /* Japanese Kanji (JIS X 0213 Plane 1) (not
                                * supported) */
#define LC_BIG5_1 0x98 /* Plane 1 Chinese traditional (not supported) */
#define LC_BIG5_2 0x99 /* Plane 1 Chinese traditional (not supported) */

/* Is a leading byte for "official" multibyte encodings? */
#define IS_LC2(c) ((unsigned char) (c) >= 0x90 && (unsigned char) (c) <= 0x99)

/*
* Postgres-specific prefix bytes for "private" single byte encodings
* (According to the MULE docs, we should be using 0x9e for this)
*/
#define LCPRV1_A 0x9a
#define LCPRV1_B 0x9b
#define IS_LCPRV1(c) ((unsigned char) (c) == LCPRV1_A || (unsigned char) (c) == LCPRV1_B)
#define IS_LCPRV1_A_RANGE(c) ((unsigned char) (c) >= 0xa0 && (unsigned char) (c) <= 0xdf)
#define IS_LCPRV1_B_RANGE(c) ((unsigned char) (c) >= 0xe0 && (unsigned char) (c) <= 0xef)

/*
* Postgres-specific prefix bytes for "private" multibyte encodings
* (According to the MULE docs, we should be using 0x9f for this)
*/
#define LCPRV2_A 0x9c
#define LCPRV2_B 0x9d
#define IS_LCPRV2(c) ((unsigned char) (c) == LCPRV2_A || (unsigned char) (c) == LCPRV2_B)
#define IS_LCPRV2_A_RANGE(c) ((unsigned char) (c) >= 0xf0 && (unsigned char) (c) <= 0xf4)
#define IS_LCPRV2_B_RANGE(c) ((unsigned char) (c) >= 0xf5 && (unsigned char) (c) <= 0xfe)

/*
* Charset IDs for private single byte encodings (0xa0-0xef)
*/
#define LC_SISHENG                                                                                                     \
    0xa0 /* Chinese SiSheng characters for
                                * PinYin/ZhuYin (not supported) */
#define LC_IPA                                                                                                         \
    0xa1 /* IPA (International Phonetic Association)
                                * (not supported) */
#define LC_VISCII_LOWER                                                                                                \
    0xa2 /* Vietnamese VISCII1.1 lower-case (not
                                * supported) */
#define LC_VISCII_UPPER                                                                                                \
    0xa3                        /* Vietnamese VISCII1.1 upper-case (not
                                * supported) */
#define LC_ARABIC_DIGIT 0xa4    /* Arabic digit (not supported) */
#define LC_ARABIC_1_COLUMN 0xa5 /* Arabic 1-column (not supported) */
#define LC_ASCII_RIGHT_TO_LEFT                                                                                         \
    0xa6 /* ASCII (left half of ISO8859-1) with
                                        * right-to-left direction (not
                                        * supported) */
#define LC_LAO                                                                                                         \
    0xa7                        /* Lao characters (ISO10646 0E80..0EDF) (not
                                * supported) */
#define LC_ARABIC_2_COLUMN 0xa8 /* Arabic 1-column (not supported) */

/*
* Charset IDs for private multibyte encodings (0xf0-0xff)
*/
#define LC_INDIAN_1_COLUMN                                                                                             \
    0xf0 /* Indian charset for 1-column width glyphs
                                * (not supported) */
#define LC_TIBETAN_1_COLUMN                                                                                            \
    0xf1 /* Tibetan 1-column width glyphs (not
                                * supported) */
#define LC_UNICODE_SUBSET_2                                                                                            \
    0xf2 /* Unicode characters of the range
                                * U+2500..U+33FF. (not supported) */
#define LC_UNICODE_SUBSET_3                                                                                            \
    0xf3 /* Unicode characters of the range
                                * U+E000..U+FFFF. (not supported) */
#define LC_UNICODE_SUBSET                                                                                              \
    0xf4                   /* Unicode characters of the range
                                * U+0100..U+24FF. (not supported) */
#define LC_ETHIOPIC 0xf5   /* Ethiopic characters (not supported) */
#define LC_CNS11643_3 0xf6 /* CNS 11643-1992 Plane 3 */
#define LC_CNS11643_4 0xf7 /* CNS 11643-1992 Plane 4 */
#define LC_CNS11643_5 0xf8 /* CNS 11643-1992 Plane 5 */
#define LC_CNS11643_6 0xf9 /* CNS 11643-1992 Plane 6 */
#define LC_CNS11643_7 0xfa /* CNS 11643-1992 Plane 7 */
#define LC_INDIAN_2_COLUMN                                                                                             \
    0xfb                /* Indian charset for 2-column width glyphs
                                * (not supported) */
#define LC_TIBETAN 0xfc /* Tibetan (not supported) */
/* #define FREE				0xfd	free (unused) */
/* #define FREE				0xfe	free (unused) */
/* #define FREE				0xff	free (unused) */

/*----------------------------------------------------
* end of MULE stuff
*----------------------------------------------------
*/

/*
* PostgreSQL encoding identifiers
*
* WARNING: the order of this enum must be same as order of entries
*			in the pg_enc2name_tbl[] array (in mb/encnames.c), and
*			in the pg_wchar_table[] array (in mb/wchar.c)!
*
*			If you add some encoding don't forget to check
*			PG_ENCODING_BE_LAST macro.
*
* PG_SQL_ASCII is default encoding and must be = 0.
*
* XXX	We must avoid renumbering any backend encoding until libpq's major
* version number is increased beyond 5; it turns out that the backend
* encoding IDs are effectively part of libpq's ABI as far as 8.2 initdb and
* psql are concerned.
*/
typedef enum pg_enc
{
    PG_SQL_ASCII = 0, /* SQL/ASCII */
    PG_EUC_JP,        /* EUC for Japanese */
    PG_EUC_CN,        /* EUC for Chinese */
    PG_EUC_KR,        /* EUC for Korean */
    PG_EUC_TW,        /* EUC for Taiwan */
    PG_EUC_JIS_2004,  /* EUC-JIS-2004 */
    PG_UTF8,          /* Unicode UTF8 */
    PG_MULE_INTERNAL, /* Mule internal code */
    PG_LATIN1,        /* ISO-8859-1 Latin 1 */
    PG_LATIN2,        /* ISO-8859-2 Latin 2 */
    PG_LATIN3,        /* ISO-8859-3 Latin 3 */
    PG_LATIN4,        /* ISO-8859-4 Latin 4 */
    PG_LATIN5,        /* ISO-8859-9 Latin 5 */
    PG_LATIN6,        /* ISO-8859-10 Latin6 */
    PG_LATIN7,        /* ISO-8859-13 Latin7 */
    PG_LATIN8,        /* ISO-8859-14 Latin8 */
    PG_LATIN9,        /* ISO-8859-15 Latin9 */
    PG_LATIN10,       /* ISO-8859-16 Latin10 */
    PG_WIN1256,       /* windows-1256 */
    PG_WIN1258,       /* Windows-1258 */
    PG_WIN866,        /* (MS-DOS CP866) */
    PG_WIN874,        /* windows-874 */
    PG_KOI8R,         /* KOI8-R */
    PG_WIN1251,       /* windows-1251 */
    PG_WIN1252,       /* windows-1252 */
    PG_ISO_8859_5,    /* ISO-8859-5 */
    PG_ISO_8859_6,    /* ISO-8859-6 */
    PG_ISO_8859_7,    /* ISO-8859-7 */
    PG_ISO_8859_8,    /* ISO-8859-8 */
    PG_WIN1250,       /* windows-1250 */
    PG_WIN1253,       /* windows-1253 */
    PG_WIN1254,       /* windows-1254 */
    PG_WIN1255,       /* windows-1255 */
    PG_WIN1257,       /* windows-1257 */
    PG_KOI8U,         /* KOI8-U */
    /* PG_ENCODING_BE_LAST points to the above entry */

    /* followings are for client encoding only */
    PG_SJIS,           /* Shift JIS (Windows-932) */
    PG_BIG5,           /* Big5 (Windows-950) */
    PG_GBK,            /* GBK (Windows-936) */
    PG_UHC,            /* UHC (Windows-949) */
    PG_GB18030,        /* GB18030 */
    PG_JOHAB,          /* EUC for Korean JOHAB */
    PG_SHIFT_JIS_2004, /* Shift-JIS-2004 */
    _PG_LAST_ENCODING_ /* mark only */

} pg_enc;

#define PG_ENCODING_BE_LAST PG_KOI8U

/*
* Please use these tests before access to pg_encconv_tbl[]
* or to other places...
*/
#define PG_VALID_BE_ENCODING(_enc) ((_enc) >= 0 && (_enc) <= PG_ENCODING_BE_LAST)

#define PG_ENCODING_IS_CLIENT_ONLY(_enc) ((_enc) > PG_ENCODING_BE_LAST && (_enc) < _PG_LAST_ENCODING_)

#define PG_VALID_ENCODING(_enc) ((_enc) >= 0 && (_enc) < _PG_LAST_ENCODING_)

/* On FE are possible all encodings */
#define PG_VALID_FE_ENCODING(_enc) PG_VALID_ENCODING(_enc)

/*
* Table for mapping an encoding number to official encoding name and
* possibly other subsidiary data.  Be careful to check encoding number
* before accessing a table entry!
*
* if (PG_VALID_ENCODING(encoding))
*		pg_enc2name_tbl[ encoding ];
*/
typedef struct pg_enc2name {
    const char* name;
    pg_enc encoding;
#ifdef WIN32
    unsigned codepage; /* codepage for WIN32 */
#endif
} pg_enc2name;

//encnames.h

#ifndef WIN32
#define DEF_ENC2NAME(name, codepage)                                                                                   \
    { #name, PG_##name }
#else
#define DEF_ENC2NAME(name, codepage)                                                                                   \
    { #name, PG_##name, codepage }
#endif

const pg_enc2name pg_enc2name_tbl[] = {
    [PG_SQL_ASCII] = DEF_ENC2NAME(SQL_ASCII, 0),
    [PG_EUC_JP] = DEF_ENC2NAME(EUC_JP, 20932),
    [PG_EUC_CN] = DEF_ENC2NAME(EUC_CN, 20936),
    [PG_EUC_KR] = DEF_ENC2NAME(EUC_KR, 51949),
    [PG_EUC_TW] = DEF_ENC2NAME(EUC_TW, 0),
    [PG_EUC_JIS_2004] = DEF_ENC2NAME(EUC_JIS_2004, 20932),
    [PG_UTF8] = DEF_ENC2NAME(UTF8, 65001),
    [PG_MULE_INTERNAL] = DEF_ENC2NAME(MULE_INTERNAL, 0),
    [PG_LATIN1] = DEF_ENC2NAME(LATIN1, 28591),
    [PG_LATIN2] = DEF_ENC2NAME(LATIN2, 28592),
    [PG_LATIN3] = DEF_ENC2NAME(LATIN3, 28593),
    [PG_LATIN4] = DEF_ENC2NAME(LATIN4, 28594),
    [PG_LATIN5] = DEF_ENC2NAME(LATIN5, 28599),
    [PG_LATIN6] = DEF_ENC2NAME(LATIN6, 0),
    [PG_LATIN7] = DEF_ENC2NAME(LATIN7, 0),
    [PG_LATIN8] = DEF_ENC2NAME(LATIN8, 0),
    [PG_LATIN9] = DEF_ENC2NAME(LATIN9, 28605),
    [PG_LATIN10] = DEF_ENC2NAME(LATIN10, 0),
    [PG_WIN1256] = DEF_ENC2NAME(WIN1256, 1256),
    [PG_WIN1258] = DEF_ENC2NAME(WIN1258, 1258),
    [PG_WIN866] = DEF_ENC2NAME(WIN866, 866),
    [PG_WIN874] = DEF_ENC2NAME(WIN874, 874),
    [PG_KOI8R] = DEF_ENC2NAME(KOI8R, 20866),
    [PG_WIN1251] = DEF_ENC2NAME(WIN1251, 1251),
    [PG_WIN1252] = DEF_ENC2NAME(WIN1252, 1252),
    [PG_ISO_8859_5] = DEF_ENC2NAME(ISO_8859_5, 28595),
    [PG_ISO_8859_6] = DEF_ENC2NAME(ISO_8859_6, 28596),
    [PG_ISO_8859_7] = DEF_ENC2NAME(ISO_8859_7, 28597),
    [PG_ISO_8859_8] = DEF_ENC2NAME(ISO_8859_8, 28598),
    [PG_WIN1250] = DEF_ENC2NAME(WIN1250, 1250),
    [PG_WIN1253] = DEF_ENC2NAME(WIN1253, 1253),
    [PG_WIN1254] = DEF_ENC2NAME(WIN1254, 1254),
    [PG_WIN1255] = DEF_ENC2NAME(WIN1255, 1255),
    [PG_WIN1257] = DEF_ENC2NAME(WIN1257, 1257),
    [PG_KOI8U] = DEF_ENC2NAME(KOI8U, 21866),
    [PG_SJIS] = DEF_ENC2NAME(SJIS, 932),
    [PG_BIG5] = DEF_ENC2NAME(BIG5, 950),
    [PG_GBK] = DEF_ENC2NAME(GBK, 936),
    [PG_UHC] = DEF_ENC2NAME(UHC, 949),
    [PG_GB18030] = DEF_ENC2NAME(GB18030, 54936),
    [PG_JOHAB] = DEF_ENC2NAME(JOHAB, 0),
    [PG_SHIFT_JIS_2004] = DEF_ENC2NAME(SHIFT_JIS_2004, 932),
};

//mbutils.c
int pg_get_client_encoding() { return pg_enc2name_tbl[PG_UTF8].encoding; }
int GetDatabaseEncoding() { return pg_enc2name_tbl[PG_UTF8].encoding; }

#endif /* PG_WCHAR_H */
