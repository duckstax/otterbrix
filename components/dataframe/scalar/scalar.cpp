#include "scalar.hpp"

#include <algorithm>
#include <string>

#include <boost/iterator/counting_iterator.hpp>

#include "core/buffer.hpp"

#include <core/date/durations.hpp>
#include <core/date/timestamps.hpp>
#include <dataframe/column/column.hpp>
#include <dataframe/detail/bitmask.hpp>

namespace components::dataframe::scalar {

    scalar_t::scalar_t(std::pmr::memory_resource* mr, data_type type, bool is_valid)
        : type_(type)
        , is_valid_(mr, is_valid) {}

    scalar_t::scalar_t(std::pmr::memory_resource* mr, scalar_t const& other)
        : type_(other.type())
        , is_valid_(mr, other.is_valid_) {}

    data_type scalar_t::type() const noexcept { return type_; }

    void scalar_t::set_valid(bool is_valid) { is_valid_.set_value(is_valid); }

    bool scalar_t::is_valid() const { return is_valid_.value(); }
    bool* scalar_t::validity_data() { return is_valid_.data(); }
    bool const* scalar_t::validity_data() const { return is_valid_.data(); }

    string_scalar::string_scalar(std::pmr::memory_resource* mr, std::string const& string, bool is_valid)
        : scalar_t(mr, data_type(type_id::string), is_valid)
        , _data(mr, string.data(), string.size()) {}

    string_scalar::string_scalar(std::pmr::memory_resource* mr, string_scalar const& other)
        : scalar_t(mr, other)
        , _data(mr, other._data) {}

    string_scalar::string_scalar(std::pmr::memory_resource* mr, core::scalar<value_type>& data, bool is_valid)
        : string_scalar(mr, data.value(), is_valid) {}

    string_scalar::string_scalar(std::pmr::memory_resource* mr, value_type const& source, bool is_valid)
        : scalar_t(mr, data_type(type_id::string), is_valid)
        , _data(mr, source.data(), source.size()) {}

    string_scalar::string_scalar(std::pmr::memory_resource* mr, core::buffer&& data, bool is_valid)
        : scalar_t(mr, data_type(type_id::string), is_valid)
        , _data(mr, std::move(data)) {}

    string_scalar::value_type string_scalar::value() const { return value_type{data(), std::size_t(size())}; }

    size_type string_scalar::size() const { return _data.size(); }

    const char* string_scalar::data() const { return static_cast<const char*>(_data.data()); }

    string_scalar::operator std::string() const { return this->to_string(); }

    std::string string_scalar::to_string() const {
        std::string result;
        result.resize(_data.size());
        std::memcpy(&result[0], _data.data(), _data.size());
        return result;
    }

    template<typename T>
    fixed_point_scalar<T>::fixed_point_scalar(std::pmr::memory_resource* mr,
                                              rep_type value,
                                              core::numbers::scale_type scale,
                                              bool is_valid)
        : scalar_t{mr, data_type{type_to_id<T>(), static_cast<int32_t>(scale)}, is_valid}
        , _data{mr, value} {}

    template<typename T>
    fixed_point_scalar<T>::fixed_point_scalar(std::pmr::memory_resource* mr, rep_type value, bool is_valid)
        : scalar_t{mr, data_type{type_to_id<T>(), 0}, is_valid}
        , _data{mr, value} {}

    template<typename T>
    fixed_point_scalar<T>::fixed_point_scalar(std::pmr::memory_resource* mr, T value, bool is_valid)
        : scalar_t{mr, data_type{type_to_id<T>(), value.scale()}, is_valid}
        , _data{mr, value.value()} {}

    template<typename T>
    fixed_point_scalar<T>::fixed_point_scalar(std::pmr::memory_resource* resource,
                                              core::scalar<rep_type>&& data,
                                              core::numbers::scale_type scale,
                                              bool is_valid)
        : scalar_t{resource, data_type{type_to_id<T>(), scale}, is_valid}
        , _data{std::move(data)} {}

    template<typename T>
    fixed_point_scalar<T>::fixed_point_scalar(std::pmr::memory_resource* resource, fixed_point_scalar<T> const& other)
        : scalar_t{resource, other}
        , _data(resource, other._data) {}

    template<typename T>
    typename fixed_point_scalar<T>::rep_type fixed_point_scalar<T>::value() const {
        return _data.value();
    }

    template<typename T>
    T fixed_point_scalar<T>::fixed_point_value() const {
        return value_type{
            core::numbers::scaled_integer<rep_type>{_data.value(), core::numbers::scale_type{type().scale()}}};
    }

    template<typename T>
    fixed_point_scalar<T>::operator value_type() const {
        return this->fixed_point_value();
    }

    template<typename T>
    typename fixed_point_scalar<T>::rep_type* fixed_point_scalar<T>::data() {
        return _data.data();
    }

    template<typename T>
    typename fixed_point_scalar<T>::rep_type const* fixed_point_scalar<T>::data() const {
        return _data.data();
    }

    template class fixed_point_scalar<core::numbers::decimal32>;
    template class fixed_point_scalar<core::numbers::decimal64>;
    template class fixed_point_scalar<core::numbers::decimal128>;

    namespace detail {

        template<typename T>
        fixed_width_scalar<T>::fixed_width_scalar(std::pmr::memory_resource* resource, T value, bool is_valid)
            : scalar_t(resource, data_type(type_to_id<T>()), is_valid)
            , _data(resource, value) {}

        template<typename T>
        fixed_width_scalar<T>::fixed_width_scalar(std::pmr::memory_resource* resource,
                                                  core::scalar<T>&& data,
                                                  bool is_valid)
            : scalar_t(resource, data_type(type_to_id<T>()), is_valid)
            , _data{std::move(data)} {}

        template<typename T>
        fixed_width_scalar<T>::fixed_width_scalar(std::pmr::memory_resource* resource,
                                                  fixed_width_scalar<T> const& other)
            : scalar_t{resource, other}
            , _data(resource, other._data) {}

        template<typename T>
        void fixed_width_scalar<T>::set_value(T value) {
            _data.set_value(value);
            this->set_valid(true);
        }

        template<typename T>
        T fixed_width_scalar<T>::value() const {
            return _data.value();
        }

        template<typename T>
        T* fixed_width_scalar<T>::data() {
            return _data.data();
        }

        template<typename T>
        T const* fixed_width_scalar<T>::data() const {
            return _data.data();
        }

        template<typename T>
        fixed_width_scalar<T>::operator value_type() const {
            return this->value();
        }

        template class fixed_width_scalar<bool>;
        template class fixed_width_scalar<int8_t>;
        template class fixed_width_scalar<int16_t>;
        template class fixed_width_scalar<int32_t>;
        template class fixed_width_scalar<int64_t>;
        ///template class fixed_width_scalar<absl::int128>;
        template class fixed_width_scalar<uint8_t>;
        template class fixed_width_scalar<uint16_t>;
        template class fixed_width_scalar<uint32_t>;
        template class fixed_width_scalar<uint64_t>;
        template class fixed_width_scalar<float>;
        template class fixed_width_scalar<double>;
        template class fixed_width_scalar<core::date::timestamp_day>;
        template class fixed_width_scalar<core::date::timestamp_s>;
        template class fixed_width_scalar<core::date::timestamp_ms>;
        template class fixed_width_scalar<core::date::timestamp_us>;
        template class fixed_width_scalar<core::date::timestamp_ns>;
        template class fixed_width_scalar<core::date::duration_day>;
        template class fixed_width_scalar<core::date::duration_s>;
        template class fixed_width_scalar<core::date::duration_ms>;
        template class fixed_width_scalar<core::date::duration_us>;
        template class fixed_width_scalar<core::date::duration_ns>;

    } // namespace detail

    template<typename T>
    numeric_scalar<T>::numeric_scalar(std::pmr::memory_resource* mr, T value, bool is_valid)
        : detail::fixed_width_scalar<T>(mr, value, is_valid) {}

    template<typename T>
    numeric_scalar<T>::numeric_scalar(std::pmr::memory_resource* mr, core::scalar<T>&& data, bool is_valid)
        : detail::fixed_width_scalar<T>(mr, std::forward<core::scalar<T>>(data), is_valid) {}

    template<typename T>
    numeric_scalar<T>::numeric_scalar(std::pmr::memory_resource* mr, numeric_scalar<T> const& other)
        : detail::fixed_width_scalar<T>{mr, other} {}

    template class numeric_scalar<bool>;
    template class numeric_scalar<int8_t>;
    template class numeric_scalar<int16_t>;
    template class numeric_scalar<int32_t>;
    template class numeric_scalar<int64_t>;
    ///template class numeric_scalar<absl::int128>;
    template class numeric_scalar<uint8_t>;
    template class numeric_scalar<uint16_t>;
    template class numeric_scalar<uint32_t>;
    template class numeric_scalar<uint64_t>;
    template class numeric_scalar<float>;
    template class numeric_scalar<double>;

    template<typename T>
    chrono_scalar<T>::chrono_scalar(std::pmr::memory_resource* mr, T value, bool is_valid)
        : detail::fixed_width_scalar<T>(mr, value, is_valid) {}

    template<typename T>
    chrono_scalar<T>::chrono_scalar(std::pmr::memory_resource* mr, core::scalar<T>&& data, bool is_valid)
        : detail::fixed_width_scalar<T>(mr, std::forward<core::scalar<T>>(data), is_valid) {}

    template<typename T>
    chrono_scalar<T>::chrono_scalar(std::pmr::memory_resource* mr, chrono_scalar<T> const& other)
        : detail::fixed_width_scalar<T>{mr, other} {}

    template class chrono_scalar<core::date::timestamp_day>;
    template class chrono_scalar<core::date::timestamp_s>;
    template class chrono_scalar<core::date::timestamp_ms>;
    template class chrono_scalar<core::date::timestamp_us>;
    template class chrono_scalar<core::date::timestamp_ns>;
    template class chrono_scalar<core::date::duration_day>;
    template class chrono_scalar<core::date::duration_s>;
    template class chrono_scalar<core::date::duration_ms>;
    template class chrono_scalar<core::date::duration_us>;
    template class chrono_scalar<core::date::duration_ns>;

    template<typename T>
    duration_scalar<T>::duration_scalar(std::pmr::memory_resource* mr, rep_type value, bool is_valid)
        : chrono_scalar<T>(mr, T{value}, is_valid) {}

    template<typename T>
    duration_scalar<T>::duration_scalar(std::pmr::memory_resource* mr, duration_scalar<T> const& other)
        : chrono_scalar<T>{mr, other} {}

    template<typename T>
    typename duration_scalar<T>::rep_type duration_scalar<T>::count() {
        return this->value().count();
    }

    template class duration_scalar<core::date::duration_day>;
    template class duration_scalar<core::date::duration_s>;
    template class duration_scalar<core::date::duration_ms>;
    template class duration_scalar<core::date::duration_us>;
    template class duration_scalar<core::date::duration_ns>;

    template<typename T>
    typename timestamp_scalar<T>::rep_type timestamp_scalar<T>::ticks_since_epoch() {
        return this->value().time_since_epoch().count();
    }

    template class timestamp_scalar<core::date::timestamp_day>;
    template class timestamp_scalar<core::date::timestamp_s>;
    template class timestamp_scalar<core::date::timestamp_ms>;
    template class timestamp_scalar<core::date::timestamp_us>;
    template class timestamp_scalar<core::date::timestamp_ns>;

    template<typename T>
    template<typename D>
    timestamp_scalar<T>::timestamp_scalar(std::pmr::memory_resource* mr, D const& value, bool is_valid)
        : chrono_scalar<T>(mr, T{typename T::duration{value}}, is_valid) {}

    template<typename T>
    timestamp_scalar<T>::timestamp_scalar(std::pmr::memory_resource* mr, timestamp_scalar<T> const& other)
        : chrono_scalar<T>{mr, other} {}

#define TS_CTOR(TimestampType, DurationType)                                                                           \
    template timestamp_scalar<TimestampType>::timestamp_scalar(std::pmr::memory_resource*, DurationType const&, bool);

    TS_CTOR(core::date::timestamp_day, core::date::duration_day)
    TS_CTOR(core::date::timestamp_day, int32_t)
    TS_CTOR(core::date::timestamp_s, core::date::duration_day)
    TS_CTOR(core::date::timestamp_s, core::date::duration_s)
    TS_CTOR(core::date::timestamp_s, int64_t)
    TS_CTOR(core::date::timestamp_ms, core::date::duration_day)
    TS_CTOR(core::date::timestamp_ms, core::date::duration_s)
    TS_CTOR(core::date::timestamp_ms, core::date::duration_ms)
    TS_CTOR(core::date::timestamp_ms, int64_t)
    TS_CTOR(core::date::timestamp_us, core::date::duration_day)
    TS_CTOR(core::date::timestamp_us, core::date::duration_s)
    TS_CTOR(core::date::timestamp_us, core::date::duration_ms)
    TS_CTOR(core::date::timestamp_us, core::date::duration_us)
    TS_CTOR(core::date::timestamp_us, int64_t)
    TS_CTOR(core::date::timestamp_ns, core::date::duration_day)
    TS_CTOR(core::date::timestamp_ns, core::date::duration_s)
    TS_CTOR(core::date::timestamp_ns, core::date::duration_ms)
    TS_CTOR(core::date::timestamp_ns, core::date::duration_us)
    TS_CTOR(core::date::timestamp_ns, core::date::duration_ns)
    TS_CTOR(core::date::timestamp_ns, int64_t)

    list_scalar::list_scalar(std::pmr::memory_resource* mr, column::column_view const& data, bool is_valid)
        : scalar_t(mr, data_type(type_id::list), is_valid)
        , _data(mr, data) {}

    list_scalar::list_scalar(std::pmr::memory_resource* mr, column::column_t&& data, bool is_valid)
        : scalar_t(mr, data_type(type_id::list), is_valid)
        , _data(std::move(data)) {}

    list_scalar::list_scalar(std::pmr::memory_resource* mr, list_scalar const& other)
        : scalar_t{mr, other}
        , _data(mr, other._data) {}

    column::column_view list_scalar::view() const { return _data.view(); }

    struct_scalar::struct_scalar(std::pmr::memory_resource* mr, struct_scalar const& other)
        : scalar_t{mr, other}
        , _data(mr, other._data) {}

    struct_scalar::struct_scalar(std::pmr::memory_resource* mr, table::table_view const& data, bool is_valid)
        : scalar_t(mr, data_type(type_id::structs), is_valid)
        , _data(mr, data) {
        init(mr, is_valid);
    }

    struct_scalar::struct_scalar(std::pmr::memory_resource* mr,
                                 core::span<column::column_view const> data,
                                 bool is_valid)
        : scalar_t(mr, data_type(type_id::structs), is_valid)
        , _data(mr, table::table_view{std::vector<column::column_view>{data.begin(), data.end()}}) {
        init(mr, is_valid);
    }

    struct_scalar::struct_scalar(std::pmr::memory_resource* mr, table::table_t&& data, bool is_valid)
        : scalar_t(mr, data_type(type_id::structs), is_valid)
        , _data(std::move(data)) {
        init(mr, is_valid);
    }

    table::table_view struct_scalar::view() const { return _data.view(); }

    void struct_scalar::init(std::pmr::memory_resource* resource, bool is_valid) {
        table::table_view tv = static_cast<table::table_view>(_data);
        assertion_exception_msg(
            std::all_of(tv.begin(), tv.end(), [](column::column_view const& col) { return col.size() == 1; }),
            "Struct scalar inputs must have exactly 1 row");
    }

} // namespace components::dataframe::scalar
