#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

#include <iterator>

#include <boost/container_hash/hash.hpp>

namespace components::dataframe {

    using size_type = int32_t;
    using bitmask_type = uint32_t;
    using valid_type = uint8_t;
    using offset_type = int32_t;

    static constexpr size_type unknown_null_count{-1};

    enum class type_id : int32_t
    {
        empty,
        int8,
        int16,
        int32,
        int64,
        uint8,
        uint16,
        uint32,
        uint64,
        float32,
        float64,
        bool8, /// 0 == false
        timestamp_days,
        timestamp_seconds,
        timestamp_milliseconds,
        timestamp_microseconds,
        timestamp_nanoseconds,
        duration_days,
        duration_seconds,
        duration_milliseconds,
        duration_microseconds,
        duration_nanoseconds,
        dictionary32, /// dictionary using int32 index
        string,
        list,
        decimal32,
        decimal64,
        decimal128,
        structs,
        num_type_ids
    };

    static_assert(int32_t(255) >= static_cast<int32_t>(type_id::num_type_ids));

    class data_type {
    public:
        data_type() = default;
        ~data_type() = default;
        data_type(data_type const&) = default;
        data_type(data_type&&) = default;
        data_type& operator=(data_type const&) = default;
        data_type& operator=(data_type&&) = default;

        explicit constexpr data_type(type_id id)
            : id_{id} {}

        explicit data_type(type_id id, int32_t scale)
            : id_{id}
            , fixed_point_scale_{scale} {
            assert(id == type_id::decimal32 || id == type_id::decimal64 || id == type_id::decimal128);
        }

        [[nodiscard]] constexpr type_id id() const noexcept { return id_; }
        [[nodiscard]] constexpr int32_t scale() const noexcept { return fixed_point_scale_; }

    private:
        type_id id_{type_id::empty};
        int32_t fixed_point_scale_{};
    };

    constexpr bool operator==(data_type const& lhs, data_type const& rhs) {
        return lhs.id() == rhs.id() && lhs.scale() == rhs.scale();
    }

    inline bool operator!=(data_type const& lhs, data_type const& rhs) { return !(lhs == rhs); }

    std::size_t size_of(data_type element_type);

} // namespace components::dataframe

namespace std {
    template<>
    struct hash<components::dataframe::data_type> {
        std::size_t operator()(const components::dataframe::data_type& s) const {
            std::size_t h1 = std::hash<std::int32_t>{}(static_cast<std::int32_t>(s.id()));
            std::size_t h2 = std::hash<std::int32_t>{}(s.scale());
            boost::hash_combine(h1, h2);
            return h1;
        }
    };
} // namespace std