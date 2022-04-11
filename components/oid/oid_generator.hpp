#pragma once

#include "oid.hpp"

template <uint SizeTimestamp, uint SizeRandom, uint SizeIncrement>
class oid_generator_t {
    static constexpr uint offset_timestamp = 0;
    static constexpr uint offset_random = offset_timestamp + SizeTimestamp;
    static constexpr uint offset_increment = offset_random + SizeRandom;
    static constexpr uint size = SizeTimestamp + SizeRandom + SizeIncrement;
    using oid_size_t = oid_t<size>;

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

    oid_size_t next() const {
        inc();
        oid_size_t oid;
        oid.fill(offset_timestamp, timestamp_, SizeTimestamp);
        oid.fill(offset_random, random_, SizeRandom);
        oid.fill(offset_increment, increment_, SizeIncrement);
        return oid;
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

    void init_random() {
        time_t t;
        srandom(static_cast<uint>(std::time(&t)));
        for (uint i = 0; i < SizeRandom; ++i) {
            random_[i] = char(random());
        }
    }

    void init_increment() {
        std::memset(increment_, 0x00, SizeIncrement);
    }

    void inc() const {
        uint i = 1;
        while (i <= SizeIncrement) {
            ++increment_[SizeIncrement - i];
            if (increment_[SizeIncrement - i] == 0) {
                ++i;
            } else {
                break;
            }
        }
    }
};
