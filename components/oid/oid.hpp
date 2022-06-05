#pragma once
#include <string>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <functional>

namespace oid {

    using byte_t = uint8_t;
    using timestamp_value_t = uint32_t;
    using random_value_t = uint64_t;
    using increment_value_t = uint32_t;

    template<class T>
    class oid_t {
        static constexpr uint size = T::size_timestamp + T::size_random + T::size_increment;
        static constexpr uint offset_timestamp = 0;
        static constexpr uint offset_random = offset_timestamp + T::size_timestamp;
        static constexpr uint offset_increment = offset_random + T::size_random;

    public:
        oid_t();
        explicit oid_t(timestamp_value_t timestamp);
        explicit oid_t(const std::string& str);
        oid_t(const oid_t& other);

        ~oid_t() = default;

        oid_t& operator=(const std::string& str);
        oid_t& operator=(const oid_t& other);

        int compare(const oid_t& other) const;
        bool is_null() const;

        const byte_t* data() const;
        std::string to_string() const;
        timestamp_value_t get_timestamp() const;

        static oid_t null();
        static oid_t max();

        bool operator==(const oid_t& other) const;
        bool operator<(const oid_t& other) const;

        static bool is_valid(const std::string& str);

        struct hash_t {
            std::size_t operator()(const oid_t& oid) const {
                return std::hash<std::string_view>()(oid.data_);
            }
        };

        struct timestamp_generator {
            static void write(byte_t *data, timestamp_value_t value) {
                for (uint i = 0; i < T::size_timestamp; ++i) {
                    data[T::size_timestamp - i - 1] = byte_t(value >> 8 * i);
                }
            }

            static void write(byte_t *data) {
                write(data, static_cast<timestamp_value_t>(time(nullptr)));
            }
        };

        struct random_generator {
            static void write(byte_t *data) {
                static random_generator generator;
                for (uint i = 0; i < T::size_random; ++i) {
                    data[T::size_random - i - 1] = byte_t(generator.value_ >> 8 * i);
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
                for (uint i = 0; i < T::size_increment; ++i) {
                    data[T::size_increment - i - 1] = byte_t(generator.value_ >> 8 * i);
                }
            }

        private:
            increment_value_t value_;

            increment_generator() {
                value_ = random();
            }
        };

    private:
        byte_t data_[size];

        void init(const std::string& str);
        void clear();
    };

    template<class T>
    inline std::ostream& operator<<(std::ostream& stream, const oid_t<T>& oid) {
        return (stream << oid.to_string());
    }


    // implementation

    inline char to_char_(uint8_t value) {
        if (value < 10) {
            return char('0' + value);
        }
        return char('a' + value - 10);
    }

    inline uint8_t from_char_(char c) {
        if (c >= '0' && c <= '9') {
            return uint8_t(c - '0');
        }
        if (c >= 'a' && c <= 'f') {
            return uint8_t(c + 10 - 'a');
        }
        return uint8_t(c + 10 - 'A');
    }

    template<class T>
    oid_t<T>::oid_t() {
        timestamp_generator::write(data_ + offset_timestamp);
        random_generator::write(data_ + offset_random);
        increment_generator::write(data_ + offset_increment);
    }

    template<class T>
    oid_t<T>::oid_t(timestamp_value_t timestamp) {
        timestamp_generator::write(data_ + offset_timestamp, timestamp);
        random_generator::write(data_ + offset_random);
        increment_generator::write(data_ + offset_increment);
    }

    template<class T>
    oid_t<T>::oid_t(const std::string& str) {
        init(str);
    }

    template<class T>
    oid_t<T>::oid_t(const oid_t& other) {
        std::memcpy(data_, other.data_, size);
    }

    template<class T>
    oid_t<T>& oid_t<T>::operator=(const std::string& str) {
        init(str);
        return *this;
    }

    template<class T>
    oid_t<T>& oid_t<T>::operator=(const oid_t& other) {
        std::memcpy(data_, other.data_, size);
        return *this;
    }

    template<class T>
    int oid_t<T>::compare(const oid_t& other) const {
        return std::memcmp(data_, other.data_, size);
    }

    template<class T>
    bool oid_t<T>::is_null() const {
        for (uint i = 0; i < size; ++i) {
            if (data_[i] > 0) {
                return false;
            }
        }
        return true;
    }

    template<class T>
    const byte_t* oid_t<T>::data() const {
        return data_;
    }

    template<class T>
    std::string oid_t<T>::to_string() const {
        char str[2 * size];
        for (uint i = 0; i < size; ++i) {
            str[2 * i] = to_char_(data_[i] / 0x10);
            str[2 * i + 1] = to_char_(data_[i] % 0x10);
        }
        return std::string(str, 2 * size);
    }

    template<class T>
    timestamp_value_t oid_t<T>::get_timestamp() const {
        timestamp_value_t value = 0;
        for (uint i = 0; i < T::size_timestamp; ++i) {
            value = (value << 8) + static_cast<timestamp_value_t>(*(data() + offset_timestamp + i));
        }
        return value;
    }

    template<class T>
    oid_t<T> oid_t<T>::null() {
        oid_t oid_null{};
        oid_null.clear();
        return oid_null;
    }

    template<class T>
    oid_t<T> oid_t<T>::max() {
        oid_t oid_max{};
        std::memset(oid_max.data_, 0xFF, size);
        return oid_max;
    }

    template<class T>
    bool oid_t<T>::operator==(const oid_t<T>& other) const {
        return compare(other) == 0;
    }

    template<class T>
    bool oid_t<T>::operator<(const oid_t<T>& other) const {
        return compare(other) < 0;
    }

    template<class T>
    bool oid_t<T>::is_valid(const std::string& str) {
        if (str.size() != 2 * size) {
            return false;
        }
        return std::all_of(str.begin(), str.end(), [](char c) {
                return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            });
    }

    template<class T>
    void oid_t<T>::init(const std::string& str) {
        if (is_valid(str)) {
            for (uint i = 0; i < size; ++i) {
                data_[i] = from_char_(str[2 * i]) * 0x10 + from_char_(str[2 * i + 1]);
            }
        } else {
            this->clear();
        }
    }

    template<class T>
    void oid_t<T>::clear() {
        std::memset(data_, 0x00, size);
    }

} //namespace oid
