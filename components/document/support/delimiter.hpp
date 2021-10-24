#pragma once

#include <ostream>

namespace storage {

class delimiter {
public:
    delimiter(const char *str =",") : _str(str) {}

    int count() const               { return _count; }
    const char* string() const      { return _str; }

    int operator++()                { return ++_count; }
    int operator++(int)             { return _count++; }

    const char* next()              { return (_count++ == 0) ? "" : _str; }

private:
    int _count = 0;
    const char* const _str;
};


static inline std::ostream& operator<< (std::ostream &out, delimiter &delim) {
    if (delim++ > 0)
        out << delim.string();
    return out;
}

}
