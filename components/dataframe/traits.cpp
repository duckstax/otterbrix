#include "traits.hpp"

#include <string_view>

#include "core/assert/assert.hpp"
#include "type_dispatcher.hpp"

namespace components::dataframe {

    namespace {

        struct unary_relationally_comparable_functor {
            template<typename T>
            inline bool operator()() const {
                return is_relationally_comparable<T, T>();
            }
        };
    } // namespace

    bool is_relationally_comparable(data_type type) {
        return type_dispatcher(type, unary_relationally_comparable_functor{});
    }

    namespace {

        struct unary_equality_comparable_functor {
            template<typename T>
            bool operator()() const {
                return is_equality_comparable<T, T>();
            }
        };
    } // namespace

    bool is_equality_comparable(data_type type) { return type_dispatcher(type, unary_equality_comparable_functor{}); }

    struct is_numeric_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_numeric<T>();
        }
    };

    bool is_numeric(data_type type) { return type_dispatcher(type, is_numeric_impl{}); }

    struct is_index_type_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_index_type<T>();
        }
    };

    bool is_index_type(data_type type) { return type_dispatcher(type, is_index_type_impl{}); }

    struct is_unsigned_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_unsigned<T>();
        }
    };

    bool is_unsigned(data_type type) { return type_dispatcher(type, is_unsigned_impl{}); }

    struct is_integral_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_integral<T>();
        }
    };

    bool is_integral(data_type type) { return type_dispatcher(type, is_integral_impl{}); }

    struct is_floating_point_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_floating_point<T>();
        }
    };

    bool is_floating_point(data_type type) { return type_dispatcher(type, is_floating_point_impl{}); }

    struct is_boolean_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_boolean<T>();
        }
    };

    bool is_boolean(data_type type) { return type_dispatcher(type, is_boolean_impl{}); }

    struct is_fixed_point_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_fixed_point<T>();
        }
    };

    bool is_fixed_point(data_type type) { return type_dispatcher(type, is_fixed_point_impl{}); }

    struct is_timestamp_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_timestamp<T>();
        }
    };

    bool is_timestamp(data_type type) { return type_dispatcher(type, is_timestamp_impl{}); }

    struct is_duration_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_duration<T>();
        }
    };

    bool is_duration(data_type type) { return type_dispatcher(type, is_duration_impl{}); }

    struct is_chrono_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_chrono<T>();
        }
    };

    bool is_chrono(data_type type) { return type_dispatcher(type, is_chrono_impl{}); }

    struct is_dictionary_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_dictionary<T>();
        }
    };

    bool is_dictionary(data_type type) { return type_dispatcher(type, is_dictionary_impl{}); }

    struct is_fixed_width_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_fixed_width<T>();
        }
    };

    bool is_fixed_width(data_type type) { return type_dispatcher(type, is_fixed_width_impl{}); }

    struct is_compound_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_compound<T>();
        }
    };

    bool is_compound(data_type type) { return type_dispatcher(type, is_compound_impl{}); }

    struct is_nested_impl {
        template<typename T>
        constexpr bool operator()() {
            return is_nested<T>();
        }
    };

    bool is_nested(data_type type) { return type_dispatcher(type, is_nested_impl{}); }

    namespace {
        template<typename FromType>
        struct is_bit_castable_to_impl {
            template<typename ToType, std::enable_if_t<is_compound<ToType>()>* = nullptr>
            constexpr bool operator()() {
                return false;
            }

            template<typename ToType, std::enable_if_t<not is_compound<ToType>()>* = nullptr>
            constexpr bool operator()() {
                if (not std::is_trivially_copyable_v<FromType> || not std::is_trivially_copyable_v<ToType>) {
                    return false;
                }
                constexpr auto from_size = sizeof(device_storage_type_t<FromType>);
                constexpr auto to_size = sizeof(device_storage_type_t<ToType>);
                return from_size == to_size;
            }
        };

        struct is_bit_castable_from_impl {
            template<typename FromType, std::enable_if_t<is_compound<FromType>()>* = nullptr>
            constexpr bool operator()(data_type) {
                return false;
            }

            template<typename FromType, std::enable_if_t<not is_compound<FromType>()>* = nullptr>
            constexpr bool operator()(data_type to) {
                return type_dispatcher(to, is_bit_castable_to_impl<FromType>{});
            }
        };
    } // namespace

    bool is_bit_castable(data_type from, data_type to) {
        return type_dispatcher(from, is_bit_castable_from_impl{}, to);
    }

} // namespace components::dataframe
