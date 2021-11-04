#include "parse_date.hpp"
#include "exception.hpp"

#include "date/date.h"
#include <stdint.h>
#include <stdarg.h>
#include <ctype.h>
#include <math.h>
#include <time.h>
#include <mutex>
#include <chrono>
#include <sstream>
#include <iomanip>

#ifdef WIN32
#include <Windows.h>
#endif

using namespace std;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace date;

constexpr int long_mounths = 0x15AA;

struct date_time_t
{
    int64_t jd;
    int year, month, day;
    int hour, minute;
    int tz;
    double sec;
    bool valid_ymd;
    bool valid_hms;
    bool valid_jd;
    bool valid_tz;
};


static int get_digits(const char *date, ...) {
    va_list ap;
    int val;
    int n;
    int min;
    int max;
    int next;
    int *val_ptr;
    int cnt = 0;
    va_start(ap, date);
    do {
        n = va_arg(ap, int);
        min = va_arg(ap, int);
        max = va_arg(ap, int);
        next = va_arg(ap, int);
        val_ptr = va_arg(ap, int*);
        val = 0;
        while(n--) {
            if(!isdigit(*date)) {
                goto end_get_digits;
            }
            val = val*10 + *date - '0';
            date++;
        }
        if (val < min || val > max || (next != 0 && next != *date)) {
            goto end_get_digits;
        }
        *val_ptr = val;
        date++;
        cnt++;
    } while (next);

end_get_digits:
    va_end(ap);
    return cnt;
}

static int parse_timezone(const char *date, date_time_t *p){
    int sgn = 0;
    int n_hour, n_minute;
    int c;
    while(isspace(*date)) { date++; }
    p->tz = 0;
    p->valid_tz = false;
    c = *date;
    if(c=='-') {
        sgn = -1;
    } else if (c=='+') {
        sgn = +1;
    } else if (c=='Z' || c=='z') {
        date++;
        goto zulu_time;
    } else {
        return c != 0;
    }
    date++;
    
    if (get_digits(date, 2, 0, 14, 0, &n_hour) != 1) {
        return 1;
    }
    
    date += 2;
    if (*date == ':') {
        date++;
    }
    
    if (get_digits(date, 2, 0, 59, 0, &n_minute) != 1) {
        return 1;
    }
    
    date += 2;
    p->tz = sgn * (n_minute + n_hour * 60);
zulu_time:
    while (isspace(*date)) { date++; }
    p->valid_tz = *date==0;
    return *date!=0;
}

static int parse_hms(const char *date, date_time_t *p){
    int h, m, s;
    double ms = 0.0;
    if (get_digits(date, 2, 0, 24, ':', &h, 2, 0, 59, 0, &m) != 2) {
        return 1;
    }
    date += 5;
    if (*date==':') {
        date++;
        if (get_digits(date, 2, 0, 59, 0, &s) != 1) {
            return 1;
        }
        date += 2;
        if (*date=='.' && isdigit(date[1])) {
            double scale = 1.0;
            date++;
            while (isdigit(*date)) {
                ms = ms*10.0 + *date - '0';
                scale *= 10.0;
                date++;
            }
            ms /= scale;
        }
    } else {
        s = 0;
    }
    p->valid_jd = false;
    p->valid_hms = true;
    p->hour = h;
    p->minute = m;
    p->sec = s + ms;
    if (parse_timezone(date, p)) return 1;
    return 0;
}

static void compute_jd(date_time_t *p){
    int y, m, d, a, b, x1, x2;

    if (p->valid_jd) return;
    if (p->valid_ymd) {
        y = p->year;
        m = p->month;
        d = p->day;
    } else {
        y = 2000;
        m = 1;
        d = 1;
    }
    if (m <= 2) {
        y--;
        m += 12;
    }
    a = y / 100;
    b = 2 - a + (a / 4);
    x1 = 36525 * (y + 4716) / 100;
    x2 = 306001 * (m + 1) / 10000;
    p->jd = (int64_t)((x1 + x2 + d + b - 1524.5 ) * 86400000);
    p->valid_jd = true;
    if (p->valid_hms) {
        p->jd += p->hour*3600000 + p->minute*60000 + (int64_t)round(p->sec*1000);
        if (p->valid_tz) {
            p->jd -= p->tz*60000;
            p->valid_ymd = false;
            p->valid_hms = false;
            p->valid_tz = false;
        }
    }
}

static inline struct tm from_date(date_time_t* p) {
    struct tm local_time {};
    local_time.tm_sec = (int)p->sec;
    local_time.tm_min = p->minute;
    local_time.tm_hour = p->hour;
    local_time.tm_mday = p->day;
    local_time.tm_mon = p->month - 1;
    local_time.tm_year = p->year - 1900;
    local_time.tm_isdst = -1;

    return local_time;
}

static void inject_local_tz(date_time_t* p)
{
    struct tm local_time = from_date(p);
    auto offset = floor<minutes>(document::get_local_timezone_offset(&local_time, false));
    p->valid_tz = true;
    p->tz = (int)offset.count();
}

static int parse_ymd(const char *date, date_time_t *p){
    int y, m, d, neg;

    if (date[0] == '-') {
        date++;
        neg = 1;
    } else {
        neg = 0;
    }
    if (get_digits(date, 4, 0, 9999, '-', &y, 2, 1, 12, '-', &m, 2, 1, 31, 0, &d) != 3) {
        return 1;
    }
    if (_usually_false(d >= 29)) {
        if (_usually_false(m == 2)) {
            if (_usually_false(d > 29)) {
                return 1;
            } else if (_usually_false(y % 4 != 0 || (y % 100 == 0 && y % 400 != 0))) {
                return 1;
            }
        } else if (_usually_false(d > 30 && !(long_mounths & (1 << m)))) {
            return 1;
        }
    }
    date += 10;
    while (isspace(*date) || 'T' == *(uint8_t*)date) { date++; }
    if (parse_hms(date, p) == 0) {
    } else if (*date == 0) {
        p->valid_hms = true;
        p->hour = p->minute = 0;
        p->sec = 0.0;
        p->valid_tz = false;
    } else {
        return 1;
    }
    p->valid_jd = false;
    p->valid_ymd = true;
    p->year = neg ? -y : y;
    p->month = m;
    p->day = d;
    if (p->valid_tz) {
        compute_jd(p);
    } else {
        inject_local_tz(p);
    }
    return 0;
}


namespace document {

int64_t parse_iso8601_date(const char* date_str) {
    date_time_t x;
    if (parse_ymd(date_str,&x))
        return invalid_date;
    compute_jd(&x);
    return x.jd - 210866760000000;
}

int64_t parse_iso8601_date(document::slice_t date_str) {
    return parse_iso8601_date(string(date_str).c_str());
}

slice_t format_iso8601_date(char buf[], int64_t time, bool utc) {
    if (time == invalid_date) {
        *buf = 0;
        return null_slice;
    }

    stringstream timestream;
    auto tp = local_time<milliseconds>(milliseconds(time));
    auto total_sec = floor<seconds>(tp);
    auto total_ms = floor<milliseconds>(tp);
    struct tm tmp_iime = from_timestamp(total_sec.time_since_epoch());
    auto offset = get_local_timezone_offset(&tmp_iime, true);
    if (utc || offset == 0min) {
        if (total_sec == total_ms) {
            timestream << format("%FT%TZ", floor<seconds>(tp));
        } else {
            timestream << format("%FT%TZ", floor<milliseconds>(tp));
        }
    } else {
        tp += offset;
        auto h = duration_cast<hours>(offset);
        auto m = offset - h;
        timestream.fill('0');
        if(total_sec == total_ms) {
            timestream << format("%FT%T", floor<seconds>(tp));
        } else {
            timestream << format("%FT%T", floor<milliseconds>(tp));
        }
        timestream << setw(3) << std::internal << showpos << h.count()
                   << noshowpos << setw(2) << (int)abs(m.count());
    }
    memcpy(buf, timestream.str().c_str(), timestream.str().size());
    return {buf, timestream.str().length()};
}

struct tm from_timestamp(seconds timestamp) {
    local_seconds tp { timestamp };
    auto dp = floor<days>(tp);
    year_month_day ymd { dp };
    auto hms = make_time(tp - dp);

    struct tm local_time {};
    local_time.tm_sec = static_cast<int>(hms.seconds().count());
    local_time.tm_min = static_cast<int>(hms.minutes().count());
    local_time.tm_hour = static_cast<int>(hms.hours().count());
    local_time.tm_mday = static_cast<int>(static_cast<unsigned>(ymd.day()));
    local_time.tm_mon = static_cast<int>(static_cast<unsigned>(ymd.month()) - 1);
    local_time.tm_year = static_cast<int>(ymd.year()) - 1900;
    local_time.tm_isdst = -1;

    return local_time;
}

seconds get_local_timezone_offset(struct tm* time, bool utc) {

#ifdef WIN32
    long s;
    _throw_if(_get_timezone(&s) != 0, document::error_code::internal_error, "Unable to query local system time zone");
    auto offset = seconds(-s);
#elif defined(__DARWIN_UNIX03) || defined(__ANDROID__) || defined(_XOPEN_SOURCE) || defined(_SVID_SOURCE)
    auto offset = seconds(-timezone);
#else
#error Unimplemented get_local_timezone_offset
#endif

    if (utc) {
        time->tm_sec -= offset.count();
    }
    if (mktime(time) != -1) {
        offset += hours(time->tm_isdst);
    }

    return offset;
}

}
