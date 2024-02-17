#pragma once

#include <core/scalar.hpp>

#include <dataframe/column/column.hpp>
#include <dataframe/table/table.hpp>
#include <dataframe/table/table_view.hpp>
#include <dataframe/traits.hpp>
#include <dataframe/types.hpp>

namespace components::dataframe::scalar {

    class scalar_t {
    public:
        virtual ~scalar_t() = default;
        scalar_t& operator=(scalar_t const& other) = delete;
        scalar_t& operator=(scalar_t&& other) = delete;

        [[nodiscard]] data_type type() const noexcept;
        void set_valid(bool is_valid);
        [[nodiscard]] bool is_valid() const;
        bool* validity_data();
        [[nodiscard]] bool const* validity_data() const;

    protected:
        data_type type_{type_id::empty};
        core::scalar<bool> is_valid_;

        scalar_t() = delete;
        scalar_t(scalar_t&& other) = default;
        scalar_t(std::pmr::memory_resource*, scalar_t const& other);
        scalar_t(std::pmr::memory_resource*, data_type type, bool is_valid = false);
    };

    namespace detail {

        template<typename T>
        class fixed_width_scalar : public scalar_t {
            static_assert(is_fixed_width<T>(), "Unexpected non-fixed-width type.");

        public:
            using value_type = T;

            ~fixed_width_scalar() override = default;
            fixed_width_scalar(fixed_width_scalar&& other) = default;
            fixed_width_scalar& operator=(fixed_width_scalar const& other) = delete;
            fixed_width_scalar& operator=(fixed_width_scalar&& other) = delete;
            fixed_width_scalar(std::pmr::memory_resource*, fixed_width_scalar const& other);

            void set_value(T value);
            explicit operator value_type() const;
            T value() const;
            T* data();
            T const* data() const;

        protected:
            core::scalar<T> _data;

            fixed_width_scalar() = delete;
            fixed_width_scalar(std::pmr::memory_resource*, T value, bool is_valid = true);
            fixed_width_scalar(std::pmr::memory_resource*, core::scalar<T>&& data, bool is_valid = true);
        };

    } // namespace detail

    template<typename T>
    class numeric_scalar : public detail::fixed_width_scalar<T> {
        static_assert(is_numeric<T>(), "Unexpected non-numeric type.");

    public:
        numeric_scalar() = delete;
        ~numeric_scalar() = default;
        numeric_scalar(numeric_scalar&& other) = default;
        numeric_scalar& operator=(numeric_scalar const& other) = delete;
        numeric_scalar& operator=(numeric_scalar&& other) = delete;

        numeric_scalar(std::pmr::memory_resource*, numeric_scalar const& other);
        numeric_scalar(std::pmr::memory_resource*, T value, bool is_valid = true);
        numeric_scalar(std::pmr::memory_resource*, core::scalar<T>&& data, bool is_valid = true);
    };

    template<typename T>
    class fixed_point_scalar : public scalar_t {
        static_assert(is_fixed_point<T>(), "Unexpected non-fixed_point type.");

    public:
        using rep_type = typename T::rep; ///< The representation type of the fixed_point numbers.
        using value_type = T;             ///< The value type of the fixed_point numbers.

        fixed_point_scalar() = delete;
        ~fixed_point_scalar() override = default;

        fixed_point_scalar(fixed_point_scalar&& other) = default;

        fixed_point_scalar& operator=(fixed_point_scalar const& other) = delete;
        fixed_point_scalar& operator=(fixed_point_scalar&& other) = delete;

        fixed_point_scalar(std::pmr::memory_resource*, fixed_point_scalar const& other);
        fixed_point_scalar(std::pmr::memory_resource*,
                           rep_type value,
                           core::numbers::scale_type scale,
                           bool is_valid = true);
        fixed_point_scalar(std::pmr::memory_resource*, rep_type value, bool is_valid = true);
        fixed_point_scalar(std::pmr::memory_resource*, T value, bool is_valid = true);
        fixed_point_scalar(std::pmr::memory_resource*,
                           core::scalar<rep_type>&& data,
                           core::numbers::scale_type scale,
                           bool is_valid = true);

        rep_type value() const;
        T fixed_point_value() const;
        explicit operator value_type() const;
        rep_type* data();
        rep_type const* data() const;

    protected:
        core::scalar<rep_type> _data;
    };

    class string_scalar : public scalar_t {
    public:
        using value_type = std::string_view;

        string_scalar() = delete;
        ~string_scalar() override = default;
        string_scalar(string_scalar&& other) = default;

        /// todo: string_scalar(string_scalar const& other) = delete;
        string_scalar& operator=(string_scalar const& other) = delete;
        string_scalar& operator=(string_scalar&& other) = delete;

        string_scalar(std::pmr::memory_resource*, string_scalar const& other);
        string_scalar(std::pmr::memory_resource*, std::string const& string, bool is_valid = true);
        string_scalar(std::pmr::memory_resource*, value_type const& source, bool is_valid = true);
        string_scalar(std::pmr::memory_resource*, core::scalar<value_type>& data, bool is_valid = true);
        string_scalar(std::pmr::memory_resource*, core::buffer&& data, bool is_valid = true);
        explicit operator std::string() const;
        [[nodiscard]] std::string to_string() const;
        [[nodiscard]] value_type value() const;
        [[nodiscard]] size_type size() const;
        [[nodiscard]] const char* data() const;

    protected:
        core::buffer _data; ///< device memory containing the string
    };

    template<typename T>
    class chrono_scalar : public detail::fixed_width_scalar<T> {
        static_assert(is_chrono<T>(), "Unexpected non-chrono type");

    public:
        chrono_scalar() = delete;
        ~chrono_scalar() = default;

        chrono_scalar(chrono_scalar&& other) = default;
        chrono_scalar& operator=(chrono_scalar const& other) = delete;
        chrono_scalar& operator=(chrono_scalar&& other) = delete;

        chrono_scalar(std::pmr::memory_resource*, chrono_scalar const& other);
        chrono_scalar(std::pmr::memory_resource*, T value, bool is_valid = true);
        chrono_scalar(std::pmr::memory_resource*, core::scalar<T>&& data, bool is_valid = true);
    };

    template<typename T>
    class timestamp_scalar : public chrono_scalar<T> {
    public:
        static_assert(is_timestamp<T>(), "Unexpected non-timestamp type");
        using chrono_scalar<T>::chrono_scalar;
        using rep_type = typename T::rep;

        timestamp_scalar() = delete;
        timestamp_scalar(timestamp_scalar&& other) = default;
        timestamp_scalar(std::pmr::memory_resource*, timestamp_scalar const& other);

        template<typename Duration2>
        timestamp_scalar(std::pmr::memory_resource*, Duration2 const& value, bool is_valid);

        rep_type ticks_since_epoch();
    };

    template<typename T>
    class duration_scalar : public chrono_scalar<T> {
    public:
        static_assert(is_duration<T>(), "Unexpected non-duration type");
        using chrono_scalar<T>::chrono_scalar;
        using rep_type = typename T::rep;

        duration_scalar() = delete;
        duration_scalar(duration_scalar&& other) = default;
        duration_scalar(std::pmr::memory_resource*, duration_scalar const& other);
        duration_scalar(std::pmr::memory_resource*, rep_type value, bool is_valid);
        rep_type count();
    };

    class list_scalar : public scalar_t {
    public:
        list_scalar() = delete;
        ~list_scalar() override = default;

        list_scalar(list_scalar&& other) = default;

        list_scalar& operator=(list_scalar const& other) = delete;
        list_scalar& operator=(list_scalar&& other) = delete;

        list_scalar(std::pmr::memory_resource*, list_scalar const& other);
        list_scalar(std::pmr::memory_resource*, column::column_view const& data, bool is_valid = true);
        list_scalar(std::pmr::memory_resource*, column::column_t&& data, bool is_valid = true);
        [[nodiscard]] column::column_view view() const;

    private:
        column::column_t _data;
    };

    class struct_scalar : public scalar_t {
    public:
        struct_scalar() = delete;
        ~struct_scalar() override = default;
        struct_scalar(struct_scalar&& other) = default;
        struct_scalar& operator=(struct_scalar const& other) = delete;
        struct_scalar& operator=(struct_scalar&& other) = delete;

        struct_scalar(std::pmr::memory_resource*, struct_scalar const& other);
        struct_scalar(std::pmr::memory_resource*, table::table_view const& data, bool is_valid = true);
        struct_scalar(std::pmr::memory_resource*, core::span<column::column_view const> data, bool is_valid = true);
        struct_scalar(std::pmr::memory_resource*, table::table_t&& data, bool is_valid = true);
        [[nodiscard]] table::table_view view() const;

    private:
        table::table_t _data;

        void init(std::pmr::memory_resource*, bool is_valid);
    };

} // namespace components::dataframe::scalar
