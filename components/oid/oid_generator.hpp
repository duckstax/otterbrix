#pragma once

#include <ctime>
#include "oid.hpp"

template <uint SizeTimestamp, uint SizeRandom, uint SizeIncrement>
class oid_generator_t {
    static constexpr uint offset_timestamp = 0;
    static constexpr uint offset_random = offset_timestamp + SizeTimestamp;
    static constexpr uint offset_increment = offset_random + SizeRandom;
    static constexpr uint size = SizeTimestamp + SizeRandom + SizeIncrement;
    using oid_size_t = oid_t<size>;

    struct initializer_random {
        initializer_random() {
            time_t t;
            srandom(static_cast<uint>(std::time(&t)));
        }
    };

public:
    oid_generator_t() {
        init_random();
        init_timestamp();
        init_increment();
    }

    explicit oid_generator_t(const oid_size_t &start_oid) {
        std::memcpy(timestamp_, start_oid.data() + offset_timestamp, SizeTimestamp);
        std::memcpy(random_, start_oid.data() + offset_random, SizeRandom);
        std::memcpy(increment_, start_oid.data() + offset_increment, SizeIncrement);
    }

    explicit oid_generator_t(const oid_generator_t &other) {
        std::memcpy(timestamp_, other.timestamp_, SizeTimestamp);
        std::memcpy(random_, other.random_, SizeRandom);
        std::memcpy(increment_, other.increment_, SizeIncrement);
    }

    oid_size_t get() const {
        oid_size_t oid;
        oid.fill(offset_timestamp, timestamp_, SizeTimestamp);
        oid.fill(offset_random, random_, SizeRandom);
        oid.fill(offset_increment, increment_, SizeIncrement);
        return oid;
    }

    oid_size_t next() const {
        inc();
        return get();
    }

private:
    char timestamp_[SizeTimestamp];
    char random_[SizeRandom];
    mutable char increment_[SizeIncrement];

    void init_timestamp() {
        time_t t;
        auto sec = std::to_string(time(&t));
        if (sec.size() > SizeTimestamp) {
            sec = sec.substr(sec.size() - SizeTimestamp);
        }
        while (sec.size() < SizeTimestamp) {
            sec += sec;
        }
        std::memcpy(timestamp_, sec.data(), SizeTimestamp);
    }

    static void initialize() {
        static initializer_random initializer;
    }

    void init_random() {
        initialize();
        for (uint i = 0; i < SizeRandom; ++i) {
            random_[i] = char(random());
        }
    }

    void init_increment() {
        for (uint i = 0; i < SizeIncrement; ++i) {
            increment_[i] = char(random());
        }
    }

    void inc() const {
        uint i = SizeIncrement - 1;
        while (i >= 0) {
            ++increment_[i];
            if (increment_[i] == 0) {
                --i;
            } else {
                break;
            }
        }
    }
};
