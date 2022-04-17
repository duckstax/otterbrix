#pragma once
#include <string>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <functional>
#include <charconv>

namespace oid {

    using byte_t = uint8_t;
    using timestamp_value_t = uint32_t;
    using random_value_t = uint64_t;
    using increment_value_t = uint32_t;

    template<uint Size>
    class oid_t {
        static constexpr uint size_timestamp = 4;
        static constexpr uint size_increment = 3;
        static constexpr uint size_random = Size - size_timestamp - size_increment;
        static constexpr uint offset_timestamp = 0;
        static constexpr uint offset_random = offset_timestamp + size_timestamp;
        static constexpr uint offset_increment = offset_random + size_random;

    public:
        oid_t();
        explicit oid_t(timestamp_value_t timestamp);
        explicit oid_t(const std::string& str);
        oid_t(const oid_t& other);

        oid_t& operator=(const std::string& str);
        oid_t& operator=(const oid_t& other);

        int compare(const oid_t& other) const;
        bool is_null() const;

        const byte_t* data() const;
        std::string to_string() const;
        timestamp_value_t get_timestamp() const;

        static oid_t null();
        static oid_t max();

        bool operator==(const oid_t<Size>& other) const;
        bool operator<(const oid_t<Size>& other) const;

        static bool is_valid(const std::string& str);

        struct hash_t {
            std::size_t operator()(const oid_t& oid) const {
                return std::hash<std::string_view>()(oid.data_);
            }
        };

        struct timestamp_generator {
            static void write(byte_t *data, timestamp_value_t value) {
                for (uint i = 0; i < size_timestamp; ++i) {
                    data[size_timestamp - i - 1] = byte_t(value >> 8 * i);
                }
            }

            static void write(byte_t *data) {
                write(data, static_cast<timestamp_value_t>(time(nullptr)));
            }
        };

        struct random_generator {
            static void write(byte_t *data) {
                static random_generator generator;
                for (uint i = 0; i < size_random; ++i) {
                    data[size_random - i - 1] = byte_t(generator.value_ >> 8 * i);
                }
            }

        private:
            random_value_t value_;

            random_generator() {
                time_t t;
                srandom(static_cast<uint>(std::time(&t)));
                value_ = random();
            }
        };

        struct increment_generator {
            static void write(byte_t *data) {
                static increment_generator generator;
                ++generator.value_;
                for (uint i = 0; i < size_increment; ++i) {
                    data[size_increment - i - 1] = byte_t(generator.value_ >> 8 * i);
                }
            }

        private:
            increment_value_t value_;

            increment_generator() {
                value_ = random();
            }
        };

    private:
        byte_t data_[Size];

        void init(const std::string& str);
        void clear();
    };

    template<uint Size>
    inline std::ostream& operator<<(std::ostream& stream, const oid_t<Size>& oid) {
        return (stream << oid.to_string());
    }


    // implementation

    template<uint Size>
    oid_t<Size>::oid_t() {
        timestamp_generator::write(data_ + offset_timestamp);
        random_generator::write(data_ + offset_random);
        increment_generator::write(data_ + offset_increment);
    }

    template<uint Size>
    oid_t<Size>::oid_t(timestamp_value_t timestamp) {
        timestamp_generator::write(data_ + offset_timestamp, timestamp);
        random_generator::write(data_ + offset_random);
        increment_generator::write(data_ + offset_increment);
    }

    template<uint Size>
    oid_t<Size>::oid_t(const std::string& str) {
        init(str);
    }

    template<uint Size>
    oid_t<Size>::oid_t(const oid_t& other) {
        std::memcpy(data_, other.data_, Size);
    }

    template<uint Size>
    oid_t<Size>& oid_t<Size>::operator=(const std::string& str) {
        init(str);
        return *this;
    }

    template<uint Size>
    oid_t<Size>& oid_t<Size>::operator=(const oid_t& other) {
        std::memcpy(data_, other.data_, Size);
        return *this;
    }

    template<uint Size>
    int oid_t<Size>::compare(const oid_t& other) const {
        return std::memcmp(data_, other.data_, Size);
    }

    template<uint Size>
    bool oid_t<Size>::is_null() const {
        for (uint i = 0; i < Size; ++i) {
            if (data_[i] > 0) {
                return false;
            }
        }
        return true;
    }

    template<uint Size>
    const byte_t* oid_t<Size>::data() const {
        return data_;
    }

    template<uint Size>
    std::string oid_t<Size>::to_string() const {
        std::string str(2 * Size, '0');
        for (uint i = 0; i < Size; ++i) {
            if (data_[i] < 16) {
                std::to_chars(str.data() + 2 * i + 1, str.data() + 2 * (i + 1), data_[i], 16);
            } else {
                std::to_chars(str.data() + 2 * i, str.data() + 2 * (i + 1), data_[i], 16);
            }
        }
        return str;
    }

    template<uint Size>
    timestamp_value_t oid_t<Size>::get_timestamp() const {
        timestamp_value_t value = 0;
        for (uint i = 0; i < size_timestamp; ++i) {
            value = (value << 8) + static_cast<timestamp_value_t>(*(data() + offset_timestamp + i));
        }
        return value;
    }

    template<uint Size>
    oid_t<Size> oid_t<Size>::null() {
        oid_t oid_null{};
        oid_null.clear();
        return oid_null;
    }

    template<uint Size>
    oid_t<Size> oid_t<Size>::max() {
        oid_t oid_max{};
        std::memset(oid_max.data_, 0xFF, Size);
        return oid_max;
    }

    template<uint Size>
    bool oid_t<Size>::operator==(const oid_t<Size>& other) const {
        return compare(other) == 0;
    }

    template<uint Size>
    bool oid_t<Size>::operator<(const oid_t<Size>& other) const {
        return compare(other) < 0;
    }

    template<uint Size>
    bool oid_t<Size>::is_valid(const std::string& str) {
        if (str.size() != 2 * Size) {
            return false;
        }
        return std::all_of(str.begin(), str.end(), [](char c) {
                return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            });
    }

    template<uint Size>
    void oid_t<Size>::init(const std::string& str) {
        if (is_valid(str)) {
            for (uint i = 0; i < Size; ++i) {
                std::from_chars(str.data() + 2 * i, str.data() + 2 * (i + 1), data_[i], 16);
            }
        } else {
            this->clear();
        }
    }

    template<uint Size>
    void oid_t<Size>::clear() {
        std::memset(data_, 0x00, Size);
    }

} //namespace oid
