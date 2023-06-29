#pragma once

namespace components::dataframe {

    namespace column {
        class column_t;
        class column_view;
    } // namespace column

    namespace dictionary {
        class dictionary_column_view;
        template<typename IndexType>
        struct dictionary_wrapper;
    } // namespace dictionary

    namespace lists {
        class list_view;
        class lists_column_view;
    } // namespace lists

    namespace scalar {
        class scalar_t;
        template<class T>
        class numeric_scalar;
        class string_scalar;
        class struct_scalar;
        class list_scalar;
        template<typename T>
        class fixed_point_scalar;
        template<typename T>
        class timestamp_scalar;
        template<typename T>
        class duration_scalar;
    } // namespace scalar

    namespace structs {
        class struct_view;
        class structs_column_view;
    } // namespace structs

    namespace table {
        class table_t;
        class table_view;
    } // namespace table

} // namespace components::dataframe