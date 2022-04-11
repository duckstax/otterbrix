#pragma once
#include <string>
#include <cstring>
#include <algorithm>
#include <functional>
#include "hex.hpp"

template <uint Size>
class oid_t {
public:
    oid_t() = default;

    oid_t(const std::string &str) {
        if (is_valid(str)) {
            for (uint i = 0; i < Size; ++i) {
                hex_to_char(str.data() + 2 * i, data_[i]);
            }
        } else {
            this->clear();
        }
    }

    oid_t(const oid_t& other) {
        std::memcpy(data_, other.data_, Size);
    }

    void clear() {
        std::memset(data_, 0x00, Size);
    }

    void fill(uint offset, const char *src, uint size) {
        std::memcpy(data_ + offset, src, size);
    }

    int compare(const oid_t &other) const {
        return std::memcmp(data_, other.data_, Size);
    }

    const char *data() const {
        return data_;
    }

    std::string to_string() const {
        std::string str(2 * Size, '0');
        for (uint i = 0; i < Size; ++i) {
            char c[2];
            char_to_hex(data_[i], c);
            str[2 * i] = c[0];
            str[2 * i + 1] = c[1];
        }
        return str;
    }

    static oid_t null() {
        oid_t oid_null{};
        oid_null.clear();
        return oid_null;
    }

    static oid_t max() {
        oid_t oid_max{};
        std::memset(oid_max.data_, 0xFF, Size);
        return oid_max;
    }

    static bool is_valid(const std::string &str) {
        if (str.size() != 2 * Size) {
            return false;
        }
        return std::all_of(str.begin(), str.end(), &is_hex);
    }

    bool operator==(const oid_t<Size> &other) const {
        return compare(other) == 0;
    }

    bool operator<(const oid_t<Size> &other) const {
        return compare(other) < 0;
    }

    struct hash_t {
        std::size_t operator()(const oid_t& oid) const {
            return std::hash<std::string_view>()(oid.data_);
        }
    };

private:
    char data_[Size];
};


template <uint Size>
inline std::ostream &operator<<(std::ostream &stream, const oid_t<Size> &oid) {
    return (stream << oid.to_string());
}
