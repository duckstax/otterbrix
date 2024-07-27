#include "cast.hpp"

#include <core/assert/assert.hpp>
#include <dataframe/column/column.hpp>
#include <dataframe/detail/bitmask.hpp>
#include <dataframe/traits.hpp>
#include <utility>

namespace {

    using namespace components::dataframe;

    template<typename From, typename To>
    constexpr inline auto is_supported_non_fixed_point_cast() {
        return is_fixed_width<To>() && !(is_fixed_point<From>() || is_fixed_point<To>()) &&
               !(is_timestamp<From>() && is_numeric<To>()) && !(is_timestamp<To>() && is_numeric<From>());
    }

    template<typename From, typename To>
    constexpr inline auto is_supported_fixed_point_cast() {
        return (is_fixed_point<From>() && is_numeric<To>()) || (is_numeric<From>() && is_fixed_point<To>()) ||
               (is_fixed_point<From>() && is_fixed_point<To>());
    }

    template<typename From, typename To>
    constexpr inline auto is_supported_cast() {
        return is_supported_non_fixed_point_cast<From, To>() || is_supported_fixed_point_cast<From, To>();
    }

    template<typename To, typename From>
    constexpr inline To unary_cast(const From element) {
        if constexpr (is_numeric<From>() && is_numeric<To>()) {
            return static_cast<To>(element);
        } else if constexpr (is_timestamp<From>() && is_timestamp<To>()) {
            return To{std::chrono::floor<To::duration>(element.time_since_epoch())};
        } else if constexpr (is_duration<From>() && is_duration<To>()) {
            return To{std::chrono::floor<To>(element)};
        } else if constexpr (is_numeric<From>() && is_duration<To>()) {
            return To{static_cast<typename To::rep>(element)};
        } else if constexpr (is_timestamp<From>() && is_duration<To>()) {
            return To{std::chrono::floor<To>(element.time_since_epoch())};
        } else if constexpr (is_duration<From>() && is_numeric<To>()) {
            return static_cast<To>(element.count());
        } else if constexpr (is_duration<From>() && is_timestamp<To>()) {
            return To{std::chrono::floor<To::duration>(element)};
        } else {
            assertion_exception_msg(false, "not valid unary cast");
        }
        return {};
    }

    template<typename To, typename From>
    constexpr inline To fixed_point_unary_cast(const From element, core::numbers::scale_type scale) {
        using condition = std::conditional_t<is_fixed_point<From>(), From, To>;
        using device_t = device_storage_type_t<condition>;
        if constexpr (is_fixed_point<From>() && is_numeric<To>()) {
            constexpr auto fp = From{core::numbers::scaled_integer<device_t>{element, scale}};
            return static_cast<To>(fp);
        } else if constexpr (is_numeric<From>() && is_fixed_point<To>()) {
            return To{element, scale}.value();
        } else {
            assertion_exception_msg(false, "not valid fixed point unary cast");
        }
        return {};
    }

    template<typename T, std::enable_if_t<is_fixed_point<T>()>* = nullptr>
    std::unique_ptr<column::column_t>
    rescale(std::pmr::memory_resource* resource, const column::column_view& input, core::numbers::scale_type scale) {
        std::make_unique<column::column_t>(resource,
                                           input.type(),
                                           input.size(),
                                           core::buffer{resource},
                                           detail::copy_bitmask(resource, input),
                                           input.null_count(resource)); //todo: impl
    }

    template<typename From, typename To>
    std::unique_ptr<column::column_t>
    dispatch_unary_cast_to(std::pmr::memory_resource* resource, const column::column_view& input, data_type type) {
        const auto size = input.size();
        auto output = std::make_unique<column::column_t>(resource,
                                                         type,
                                                         size,
                                                         core::buffer{resource},
                                                         detail::copy_bitmask(resource, input),
                                                         input.null_count(resource));
        column::mutable_column_view output_mutable = *output;
        if constexpr (is_supported_non_fixed_point_cast<From, To>()) {
            std::transform(input.begin<From>(), input.end<From>(), output_mutable.begin<To>(), unary_cast<To>);
        } else if constexpr (is_fixed_point<From>() && is_numeric<To>()) {
            using device_t = device_storage_type_t<From>;
            const auto scale = core::numbers::scale_type{input.type().scale()};
            std::transform(input.begin<device_t>(), input.end<device_t>(), output_mutable.begin<To>(), [scale](From v) {
                return fixed_point_unary_cast<To>(v, scale);
            });
        } else if constexpr (is_numeric<From>() && is_fixed_point<To>()) {
            using device_t = device_storage_type_t<To>;
            const auto scale = core::numbers::scale_type{type.scale()};
            std::transform(input.begin<From>(), input.end<From>(), output_mutable.begin<device_t>(), [scale](From v) {
                return fixed_point_unary_cast<From, To>(v, scale);
            });
        } else if constexpr (is_fixed_point<From>() && is_fixed_point<To>() && std::is_same_v<From, To>) {
            if (input.type() == type) {
                return std::make_unique<column::column_t>(resource, input);
            }
            return rescale<To>(resource, input, core::numbers::scale_type{type.scale()});
        } else if constexpr (is_fixed_point<From>() && is_fixed_point<To>() && !std::is_same_v<From, To>) {
            using from_device_t = device_storage_type_t<From>;
            using to_device_t = device_storage_type_t<To>;
            if (input.type().scale() == type.scale()) {
                std::transform(input.begin<from_device_t>(),
                               input.end<from_device_t>(),
                               output_mutable.begin<to_device_t>(),
                               [](from_device_t e) { return static_cast<to_device_t>(e); });
            } else {
                if constexpr (sizeof(from_device_t) < sizeof(to_device_t)) {
                    std::transform(input.begin<from_device_t>(),
                                   input.end<from_device_t>(),
                                   output_mutable.begin<to_device_t>(),
                                   [](from_device_t e) { return static_cast<to_device_t>(e); });
                    return rescale<To>(resource, output_mutable, core::numbers::scale_type{type.scale()});
                } else {
                    auto temporary = rescale<From>(resource, input, core::numbers::scale_type{type.scale()});
                    return detail::cast(resource, *temporary, type);
                }
            }
        } else if constexpr (!is_fixed_width<To>()) {
            assertion_exception_msg(false, "Column type must be numeric or chrono or decimal32/64/128");
        } else if constexpr (is_fixed_point<From>()) {
            assertion_exception_msg(false, "Currently only decimal32/64/128 to floating point/integral is supported");
        } else if constexpr (is_timestamp<From>() && is_numeric<To>()) {
            assertion_exception_msg(false, "Timestamps can be created only from duration");
        } else {
            assertion_exception_msg(false, "not valid dispatch unary cast");
        }
        return output;
    }

#define release_func
#ifdef release_func
    template<typename T>
    std::unique_ptr<column::column_t>
    dispatch_unary_cast_from(std::pmr::memory_resource* resource, const column::column_view& input, data_type type) {
        if constexpr (is_fixed_width<T>()) {
            return type_dispatcher(type, dispatch_unary_cast_to<T>, resource, input, type);
        } else {
            assertion_exception_msg(false, "Column type must be numeric or chrono or decimal32/64/128");
        }
    }
#else
    struct dispatch_unary_cast_from {
        column::column_view input;

        explicit dispatch_unary_cast_from(const column::column_view& inp)
            : input(inp) {}

        template<typename T, std::enable_if_t<is_fixed_width<T>()>* = nullptr>
        std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource, data_type type) {
            return type_dispatcher(type, dispatch_unary_cast_to<T>, resource, input, type);
        }

        template<typename T, typename... Args>
        std::enable_if_t<!is_fixed_width<T>(), std::unique_ptr<column::column_t>> operator()(Args&&...) {
            assertion_exception_msg(false, "Column type must be numeric or chrono or decimal32/64/128");
        }
    };
#endif

} // namespace

namespace components::dataframe::detail {

    std::unique_ptr<column::column_t>
    cast(std::pmr::memory_resource* resource, const column::column_view& input, data_type type) {
        assertion_exception_msg(is_fixed_width(type), "Unary cast type must be fixed-width.");
#ifdef release_func
        //return type_dispatcher(input.type(), dispatch_unary_cast_from, resource, input, type);
#else
        return type_dispatcher(input.type(), dispatch_unary_cast_from{input}, resource, type);
#endif
    }

} // namespace components::dataframe::detail
